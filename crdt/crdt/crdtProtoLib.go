package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"unsafe"
)

//This file contains the conversion to and from protobufs of ops, read args and states
//The CRDT itself doesn't have to implement any interface from here. Neither do effects or other internal structures besides the ones mentioned below
//The update operations have to implement ProtoUpd
//Downstream operations have to implement ProtoDownUpd, for replication purposes only.
//Read operations (including StateReadArguments) have to implement ProtoRead.
//States have to implement ProtoState
//Conversion of protobuf -> op/state/arg is done by "Global functions" (e.g., UpdateProtoToAntidoteUpdate)

/*
INDEX:
	INTERFACES
	GLOBAL FUNCS
	GLOBAL HELPER FUNCS
		SELECTION HELPERS
		OTHER HELPERS
	GENERIC
	MISCELANEOUS
*/

//NOTE: Maybe think of some way to avoid requiring the generic methods?
//Maybe some kind of array or map built at runtime?

// *****INTERFACES*****/
type ProtoUpd interface {
	ToUpdateObject() (protobuf *proto.ApbUpdateOperation)

	FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments)
}

type ProtoRead interface {
	ToPartialRead() (protobuf *proto.ApbPartialReadArgs)

	FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments)
}

type ProtoState interface {
	ToReadResp() (protobuf *proto.ApbReadObjectResp)

	FromReadResp(proto *proto.ApbReadObjectResp) (state State)
}

type ProtoDownUpd interface {
	ToReplicatorObj() (protobuf *proto.ProtoOpDownstream)

	FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments)
}

type ProtoCRDT interface {
	ToProtoState() (protobuf *proto.ProtoState)

	FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID uint16) (newCRDT CRDT)
}

/*****GLOBAL FUNCS*****/
func UpdateProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation, crdtType proto.CRDTType) (op UpdateArguments) {
	//fmt.Println("[CRDTProtoLib]Proto->Antidote. CRDTType: ", crdtType)
	specialOp := protobuf.GetSpecialop()

	/*if protobuf.Resetop != nil {
		return ResetOp{}
	}
	if protobuf.Multiupdop != nil {
		return MultiUpd{}.FromUpdateObject(protobuf)
	}*/

	if specialOp == proto.SPECIAL_UPD_NORMAL { //If specialOp is not set, it will default to this.
		switch crdtType {
		case proto.CRDTType_COUNTER:
			return Increment{}.FromUpdateObject(protobuf)
		case proto.CRDTType_LWWREG:
			return SetValue{}.FromUpdateObject(protobuf)
		case proto.CRDTType_COUNTER_FLOAT:
			return IncrementFloat{}.FromUpdateObject(protobuf)
		case proto.CRDTType_ORSET:
			return updateSetProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_ORMAP:
			return updateMapProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_RRMAP:
			return updateEmbMapProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_TOPK_RMV, proto.CRDTType_TOPK_RMV_EXT:
			return updateTopkRmvProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_AVG:
			return AddMultipleValue{}.FromUpdateObject(protobuf)
		case proto.CRDTType_MAXMIN:
			return updateMaxMinProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_TOPSUM:
			return updateTopsProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_TOPK:
			return updateTopkProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_FLAG_EW, proto.CRDTType_FLAG_DW, proto.CRDTType_FLAG_LWW:
			return updateFlagProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_FATCOUNTER:
			return updateBCounterProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_PAIR_COUNTER:
			return updatePairCounterProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_ARRAY_COUNTER:
			return updateArrayCounterProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_ARRAY_FLOAT:
			return updateArrayFloatProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_MULTI_ARRAY:
			return updateMultiArrayProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_MVREG:
			return MVSetValue{}.FromUpdateObject(protobuf)
		case proto.CRDTType_SIMPLE_DATE, proto.CRDTType_SETW_DATE, proto.CRDTType_INCW_DATE, proto.CRDTType_SET_ONLY_DATE:
			return updateDateProtoToAntidoteUpdate(protobuf, crdtType)
		case proto.CRDTType_ARRAY_COMPACT:
			return updateCompactArrayProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_ARRAY_STRING:
			return updateStringArrayProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_MAP_COUNTER:
			return updateMapCounterProtoToAntidoteUpdate(protobuf)
		case proto.CRDTType_ARRAY_BYTE:
			return updateArrayByteProtoToAntidoteUpdate(protobuf)
		}
	} else if specialOp == proto.SPECIAL_UPD_RESET {
		return ResetOp{}
	} else if specialOp == proto.SPECIAL_UPD_MULTI {
		return MultiUpd{}.FromUpdateObject(protobuf)
	} //No further cases.
	return nil
}

func PartialReadOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs, crdtType proto.CRDTType, readType proto.READType) (read ReadArguments) {
	var tmpRead ReadArguments = nil
	//fmt.Printf("[CRDTProtoLib][PartRead->AntidoteRead]CrdtType: %v, ReadType: %v, Protobuf: %+v\n", crdtType, readType, protobuf)

	switch readType {
	case proto.READType_FULL:
		tmpRead = StateReadArguments{}

	//Set
	case proto.READType_LOOKUP:
		tmpRead = LookupReadArguments{}.FromPartialRead(protobuf)
		//tmpRead = LookupReadArguments{Elem: Element(protobuf.GetSet().GetLookup().GetElement())}
	case proto.READType_N_ELEMS:
		tmpRead = GetNElementsArguments{}.FromPartialRead(protobuf)

	//Maps
	case proto.READType_HAS_KEY:
		tmpRead = HasKeyArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_KEYS:
		tmpRead = GetKeysArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_VALUE:
		tmpRead = partialGetValueOpToAntidoteRead(protobuf, crdtType)
	case proto.READType_GET_VALUES:
		tmpRead = partialGetValuesOpToAntidoteRead(protobuf, crdtType)
	case proto.READType_GET_ALL_VALUES:
		//fmt.Printf("[CRDTProtoLib][PartRead->AntidoteRead]Making EmbMapPartialOnAllArguments.")
		tmpRead = EmbMapPartialOnAllArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_COND:
		tmpRead = EmbMapConditionalReadArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_ALL_COND:
		tmpRead = EmbMapConditionalReadAllArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_EXCEPT:
		tmpRead = EmbMapExceptArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_EXCEPT_COND:
		tmpRead = EmbMapConditionalReadExceptArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_EXCEPT_SINGLE:
		tmpRead = EmbMapSingleExceptArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_EXCEPT_SINGLE_COND:
		tmpRead = EmbMapConditionalReadExceptSingleArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_AGGREGATE:
		tmpRead = EmbMapAggregateArguments{}.FromPartialRead(protobuf)

	//Topk/TopSum
	case proto.READType_GET_N:
		tmpRead = GetTopNArguments{}.FromPartialRead(protobuf)
		//tmpRead = partialGetNOpToAntidoteRead(protobuf, crdtType)
	case proto.READType_GET_ABOVE_VALUE:
		tmpRead = GetTopKAboveValueArguments{}.FromPartialRead(protobuf)
		//tmpRead = partialGetAboveValueOpToAntidoteRead(protobuf, crdtType)
	case proto.READType_TOP_AGGR:
		tmpRead = TopAggregateArguments{}.FromPartialRead(protobuf)

	//Avg
	case proto.READType_GET_FULL_AVG:
		tmpRead = AvgGetFullArguments{}.FromPartialRead(protobuf)

	//PairCounter
	case proto.READType_PAIR_FIRST:
		tmpRead = ReadFirstArguments{}.FromPartialRead(protobuf)
	case proto.READType_PAIR_SECOND:
		tmpRead = ReadSecondArguments{}.FromPartialRead(protobuf)

	//ArrayCounter
	case proto.READType_COUNTER_SINGLE:
		tmpRead = CounterArraySingleArguments(0).FromPartialRead(protobuf)
	case proto.READType_COUNTER_EXCEPT:
		tmpRead = CounterArrayExceptArguments(0).FromPartialRead(protobuf)
	case proto.READType_COUNTER_SUB:
		tmpRead = CounterArraySubArguments{}.FromPartialRead(protobuf)
	case proto.READType_COUNTER_EXCEPT_RANGE:
		tmpRead = CounterArrayExceptRangeArguments{}.FromPartialRead(protobuf)

	//FloatCounter
	case proto.READType_FLOAT_SINGLE:
		tmpRead = FloatArraySingleArguments(0).FromPartialRead(protobuf)
	case proto.READType_FLOAT_EXCEPT:
		tmpRead = FloatArrayExceptArguments(0).FromPartialRead(protobuf)
	case proto.READType_FLOAT_SUB:
		tmpRead = FloatArraySubArguments{}.FromPartialRead(protobuf)
	case proto.READType_FLOAT_RANGE:
		tmpRead = FloatArrayRangeArguments{}.FromPartialRead(protobuf)
	case proto.READType_FLOAT_EXCEPT_RANGE:
		tmpRead = FloatArrayExceptRangeArguments{}.FromPartialRead(protobuf)

	//MultiArray
	case proto.READType_MULTI_DATA_INT:
		tmpRead = MultiArrayDataIntArguments(0).FromPartialRead(protobuf)
	case proto.READType_MULTI_DATA_COND:
		tmpRead = partialMultiDataCondOpToAntidoteRead(protobuf)
	case proto.READType_MULTI_COND:
		tmpRead = MultiArrayComparableArguments{}.FromPartialRead(protobuf)
	case proto.READType_MULTI_AGGR:
		tmpRead = MultiArrayAggrArguments{}.FromPartialRead(protobuf)
	case proto.READType_MULTI_FULL:
		tmpRead = partialMultiFullOpToAntidoteRead(protobuf)
	case proto.READType_MULTI_CUSTOM:
		tmpRead = MultiArrayCustomArguments{}.FromPartialRead(protobuf)
	case proto.READType_MULTI_SINGLE:
		tmpRead = partialMultiSingleOpToAntidoteRead(protobuf)
	case proto.READType_MULTI_RANGE:
		tmpRead = partialMultiRangeOpToAntidoteRead(protobuf)
	case proto.READType_MULTI_SUB:
		tmpRead = partialMultiSubOpToAntidoteRead(protobuf)

	//Reg (MV)
	case proto.READType_GET_SINGLE:
		tmpRead = MVRegisterSingleReadArguments{}.FromPartialRead(protobuf)

	//Date
	case proto.READType_DATE_FULL:
		tmpRead = DateFullArguments{}.FromPartialRead(protobuf)
	case proto.READType_DATE_ONLY:
		tmpRead = DateOnlyArguments{}.FromPartialRead(protobuf)
	case proto.READType_DATE_TIME_ONLY:
		tmpRead = TimeArguments{}.FromPartialRead(protobuf)
	case proto.READType_DATE_TIMESTAMP:
		tmpRead = TimestampArguments{}.FromPartialRead(protobuf)

	//CompactArray
	case proto.READType_ARRAY_COMPACT_POS:
		tmpRead = partialCompactArrayPosToAntidoteRead(protobuf)
	case proto.READType_ARRAY_COMPACT_EXCEPT:
		tmpRead = CompactArrayExceptArguments(0).FromPartialRead(protobuf)
	case proto.READType_ARRAY_COMPACT_RANGE:
		tmpRead = CompactArrayRangeArguments{}.FromPartialRead(protobuf)
	case proto.READType_ARRAY_COMPACT_SUB:
		tmpRead = CompactArraySubArguments(nil).FromPartialRead(protobuf)

	//StringArray
	case proto.READType_ARRAY_STRING_POS:
		tmpRead = StringArraySingleArguments(0).FromPartialRead(protobuf)
	case proto.READType_ARRAY_STRING_EXCEPT:
		tmpRead = StringArrayExceptArguments(0).FromPartialRead(protobuf)
	case proto.READType_ARRAY_STRING_RANGE:
		tmpRead = StringArrayRangeArguments{}.FromPartialRead(protobuf)
	case proto.READType_ARRAY_STRING_SUB:
		tmpRead = StringArraySubArguments(nil).FromPartialRead(protobuf)

	//ByteArray
	case proto.READType_ARRAY_BYTE_POS:
		tmpRead = byteArrayPosToAntidoteRead(protobuf)
	case proto.READType_ARRAY_BYTE_RANGE:
		tmpRead = ByteArrayRangeArguments{}.FromPartialRead(protobuf)
	case proto.READType_ARRAY_BYTE_EXCEPT:
		tmpRead = ByteArrayExceptArguments(0).FromPartialRead(protobuf)
	case proto.READType_ARRAY_BYTE_SUB:
		tmpRead = ByteArraySubArguments{}.FromPartialRead(protobuf)

	//MapCounter
	case proto.READType_MAP_COUNTER_VALUE:
		tmpRead = partialMapCounterValueToAntidoteRead(protobuf)
	case proto.READType_MAP_COUNTER_HAS_KEY:
		tmpRead = partialMapCounterHasKeyToAntidoteRead(protobuf)
	case proto.READType_MAP_COUNTER_KEYS:
		tmpRead = partialMapCounterKeysToAntidoteRead(protobuf)
	case proto.READType_MAP_COUNTER_VALUES:
		tmpRead = partialMapCounterValuesToAntidoteRead(protobuf)
	case proto.READType_MAP_COUNTER_COMP:
		tmpRead = partialMapCounterCompToAntidoteRead(protobuf)

	//Process
	case proto.READType_PROCESS:
		tmpRead = ReadProcessingObjectParams{}.FromPartialRead(protobuf)
	}

	return tmpRead
}

func ReadRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType, readType proto.READType) (state State) {
	//ConvertProtoObjectToAntidoteState
	//fmt.Printf("[CRDTProtoLib]ReadRespProtoToAntidoteState. CrdtType: %v, ReadType: %v. Protobuf: %+v\n", crdtType, readType, protobuf)
	//fmt.Printf("[CRDTProtoLib]ReadRespProtoToAntidoteState. CrdtType: %v, ReadType: %v\n", crdtType, readType)
	if readType != proto.READType_FULL {
		//fmt.Println("[CRDTProtoLib]ReadRespProtoToAntidoteState. Processing as partial read.")
		return partialReadRespProtoToAntidoteState(protobuf, crdtType, readType)
	}
	//fmt.Println("[CRDTProtoLib]ReadRespProtoToAntidoteState. Processing as full read.")

	switch crdtType {
	case proto.CRDTType_COUNTER:
		state = CounterState(0).FromReadResp(protobuf)
	case proto.CRDTType_LWWREG:
		state = RegisterState{}.FromReadResp(protobuf)
	case proto.CRDTType_COUNTER_FLOAT:
		state = CounterFloatState(0.0).FromReadResp(protobuf)
	case proto.CRDTType_ORSET:
		state = SetAWValueState{}.FromReadResp(protobuf)
	case proto.CRDTType_ORMAP:
		state = MapEntryState{}.FromReadResp(protobuf)
	case proto.CRDTType_RRMAP:
		state = EmbMapEntryState{}.FromReadResp(protobuf)
	case proto.CRDTType_TOPK_RMV, proto.CRDTType_TOPSUM, proto.CRDTType_TOPK, proto.CRDTType_TOPK_RMV_EXT:
		state = TopKValueState{}.FromReadResp(protobuf)
	case proto.CRDTType_AVG:
		state = AvgState{}.FromReadResp(protobuf)
	case proto.CRDTType_MAXMIN:
		state = MaxMinState{}.FromReadResp(protobuf)
	//case proto.CRDTType_TOPSUM:
	//state = TopSValueState{}.FromReadResp(protobuf)
	case proto.CRDTType_FLAG_EW:
		state = FlagState{}.FromReadResp(protobuf)
	case proto.CRDTType_PAIR_COUNTER:
		state = PairCounterState{}.FromReadResp(protobuf)
	case proto.CRDTType_ARRAY_COUNTER:
		state = CounterArrayState{}.FromReadResp(protobuf)
	case proto.CRDTType_ARRAY_FLOAT:
		state = FloatArrayCRDTState{}.FromReadResp(protobuf)
	case proto.CRDTType_MULTI_ARRAY:
		state = MultiArrayState{}.FromReadResp(protobuf)
	case proto.CRDTType_MVREG:
		state = MVRegisterState{}.FromReadResp(protobuf)
	case proto.CRDTType_SIMPLE_DATE, proto.CRDTType_SETW_DATE, proto.CRDTType_INCW_DATE, proto.CRDTType_SET_ONLY_DATE:
		state = DateState{}.FromReadResp(protobuf)
	case proto.CRDTType_ARRAY_COMPACT:
		state = CompactArrayState{}.FromReadResp(protobuf)
	case proto.CRDTType_ARRAY_STRING:
		state = StringArrayState{}.FromReadResp(protobuf)
	case proto.CRDTType_MAP_COUNTER:
		state = stateMapCounterProtoToAntidoteState(protobuf)
	}

	return
}

func partialReadRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType, readType proto.READType) (state State) {
	//fmt.Printf("[CRDTProtoLib]partialReadRespProtoToAntidoteState. CrdtType: %v, ReadType: %v\n", crdtType, readType)
	switch readType {
	//Sets
	case proto.READType_LOOKUP:
		state = SetAWLookupState{}.FromReadResp(protobuf)
	case proto.READType_N_ELEMS:
		state = SetAWNElementsState{}.FromReadResp(protobuf)

	//Maps
	case proto.READType_HAS_KEY:
		state = partialHasKeyRespProtoToAntidoteState(protobuf, crdtType)
	case proto.READType_GET_KEYS:
		state = partialGetKeysRespProtoToAntidoteState(protobuf, crdtType)
	case proto.READType_GET_VALUE:
		state = partialGetValueRespProtoToAntidoteState(protobuf, crdtType)
	case proto.READType_GET_VALUES:
		state = partialGetValuesRespProtoToAntidoteState(protobuf, crdtType)
	case proto.READType_GET_ALL_VALUES, proto.READType_GET_EXCEPT, proto.READType_GET_EXCEPT_SINGLE:
		state = EmbMapGetValuesState{}.FromReadResp(protobuf)
	case proto.READType_GET_COND, proto.READType_GET_ALL_COND,
		proto.READType_GET_EXCEPT_COND, proto.READType_GET_EXCEPT_SINGLE_COND:
		state = EmbMapEntryState{}.FromReadResp(protobuf)
	case proto.READType_GET_AGGREGATE:
		state = partialGetAggregateRespProtoToAntidoteState(protobuf)

	//Topk
	case proto.READType_GET_N, proto.READType_GET_ABOVE_VALUE:
		//state = TopKValueState{}.FromReadResp(protobuf)
		state = partialTopRespProtoToAntidoteState(protobuf, crdtType)
	case proto.READType_TOP_AGGR:
		state = partialTopAggrRespProtoToAntidoteState(protobuf)

	//Avg
	case proto.READType_GET_FULL_AVG:
		state = AvgFullState{}.FromReadResp(protobuf)

	//PairCounter
	case proto.READType_PAIR_FIRST:
		state = SingleFirstCounterState(0).FromReadResp(protobuf)
	case proto.READType_PAIR_SECOND:
		state = SingleSecondCounterState(0.0).FromReadResp(protobuf)

	//ArrayCounter
	case proto.READType_COUNTER_SINGLE:
		state = CounterArraySingleState(0).FromReadResp(protobuf)
	case proto.READType_COUNTER_EXCEPT, proto.READType_COUNTER_SUB, proto.READType_COUNTER_EXCEPT_RANGE:
		state = CounterArrayState{}.FromReadResp(protobuf)

	//ArrayFloat
	case proto.READType_FLOAT_SINGLE:
		state = FloatArraySingleState(0).FromReadResp(protobuf)
	case proto.READType_FLOAT_EXCEPT, proto.READType_FLOAT_SUB, proto.READType_FLOAT_EXCEPT_RANGE:
		state = FloatArrayState{}.FromReadResp(protobuf)

	//MultiArray
	case proto.READType_MULTI_DATA_COND:
		state = partialMultiDataCondRespToAntidoteState(protobuf)
	case proto.READType_MULTI_COND:
		state = MultiArrayState{}.FromReadResp(protobuf)
	case proto.READType_MULTI_FULL, proto.READType_MULTI_RANGE, proto.READType_MULTI_SUB: //They all use the same states
		state = partialMultiFullRespToAntidoteRead(protobuf)
	case proto.READType_MULTI_CUSTOM:
		state = MultiArrayState{}.FromReadResp(protobuf)
	case proto.READType_MULTI_SINGLE:
		state = partialMultiSingleRespToAntidoteRead(protobuf)

	//Reg
	case proto.READType_GET_SINGLE:
		state = MVRegisterSingleState{}.FromReadResp(protobuf)

	//Date
	case proto.READType_DATE_FULL:
		state = DateFullState{}.FromReadResp(protobuf)
	case proto.READType_DATE_ONLY:
		state = DateOnlyState{}.FromReadResp(protobuf)
	case proto.READType_DATE_TIME_ONLY:
		state = TimeState{}.FromReadResp(protobuf)
	case proto.READType_DATE_TIMESTAMP:
		state = TimestampState(0).FromReadResp(protobuf)

	//CompactArray
	case proto.READType_ARRAY_COMPACT_POS:
		state = partialCompactArrayRespToAntidoteRead(protobuf)
	case proto.READType_ARRAY_COMPACT_EXCEPT, proto.READType_ARRAY_COMPACT_RANGE, proto.READType_ARRAY_COMPACT_SUB:
		state = CompactArrayState{}.FromReadResp(protobuf)

	//StringArray
	case proto.READType_ARRAY_STRING_POS:
		state = StringArraySingleState("").FromReadResp(protobuf)
	case proto.READType_ARRAY_STRING_EXCEPT, proto.READType_ARRAY_STRING_RANGE, proto.READType_ARRAY_STRING_SUB:
		state = StringArrayState{}.FromReadResp(protobuf)

	//MapCounter
	case proto.READType_MAP_COUNTER_VALUE:
		state = partialCounterMapValueRespToAntidoteState(protobuf)
	case proto.READType_MAP_COUNTER_KEYS:
		state = partialCounterMapKeysRespToAntidoteState(protobuf)
	case proto.READType_MAP_COUNTER_HAS_KEY:
		state = partialCounterMapHasKeyRespToAntidoteState(protobuf)
	case proto.READType_MAP_COUNTER_COMP: //This will only happen if read the map partially (i.e., ignore keys or values)
		state = partialCounterMapCompRespToAntidoteState(protobuf)
	}

	return
}

func DownstreamProtoToAntidoteDownstream(protobuf *proto.ProtoOpDownstream, crdtType proto.CRDTType) (downOp DownstreamArguments) {
	/*if protobuf.GetTopkinitOp() != nil {
		downOp = TopKInit{}.FromReplicatorObj(protobuf)
		return downOp
	}*/
	specialOp := protobuf.GetSpecialop()
	/*if protobuf.ResetOp != nil {
		return ResetOp{}
	}
	if protobuf.MultiUpdOp != nil {
		return MultiUpd{}.FromReplicatorObj(protobuf)
	}*/
	if specialOp == proto.SPECIAL_UPD_NORMAL { //If specialOp is not set, it will default to this.
		switch crdtType {
		case proto.CRDTType_COUNTER:
			downOp = downstreamProtoCounterToAntidoteDownstream(protobuf)
		case proto.CRDTType_LWWREG:
			downOp = DownstreamSetValue{}.FromReplicatorObj(protobuf)
		case proto.CRDTType_COUNTER_FLOAT:
			downOp = downstreamProtoCounterFloatToAntidoteDownstream(protobuf)
		case proto.CRDTType_ORSET:
			downOp = downstreamProtoSetToAntidoteDownstream(protobuf)
		case proto.CRDTType_ORMAP:
			downOp = downstreamProtoORMapToAntidoteDownstream(protobuf)
		case proto.CRDTType_RRMAP:
			downOp = downstreamProtoRRMapToAntidoteDownstream(protobuf)
		case proto.CRDTType_TOPK_RMV:
			downOp = downstreamProtoTopKRmvToAntidoteDownstream(protobuf)
		case proto.CRDTType_AVG:
			downOp = AddMultipleValue{}.FromReplicatorObj(protobuf)
		case proto.CRDTType_MAXMIN:
			downOp = downstreamProtoMaxMinToAntidoteDownstream(protobuf)
		case proto.CRDTType_TOPSUM:
			//downOp = DownstreamTopSAdd{}.FromReplicatorObj(protobuf)
			downOp = downstreamProtoTopSToAntidoteDownstream(protobuf)
		case proto.CRDTType_TOPK:
			downOp = downstreamProtoTopKToAntidoteDownstream(protobuf)
		case proto.CRDTType_FLAG_EW:
			downOp = downstreamProtoFlagEWToAntidoteDownstream(protobuf)
		case proto.CRDTType_FLAG_DW:
			downOp = downstreamProtoFlagDWToAntidoteDownstream(protobuf)
		case proto.CRDTType_FLAG_LWW:
			downOp = downstreamProtoFlagLWWToAntidoteDownstream(protobuf)
		case proto.CRDTType_FATCOUNTER:
			downOp = downstreamProtoBCounterToAntidoteDownstream(protobuf)
		case proto.CRDTType_PAIR_COUNTER:
			downOp = downstreamProtoPairCounterToAntidoteDownstream(protobuf)
		case proto.CRDTType_ARRAY_COUNTER:
			downOp = downstreamProtoCounterArrayToAntidoteDownstream(protobuf)
		case proto.CRDTType_ARRAY_FLOAT:
			downOp = downstreamProtoFloatArrayToAntidoteDownstream(protobuf)
		case proto.CRDTType_MULTI_ARRAY:
			downOp = downstreamProtoMultiArrayToAntidoteDownstream(protobuf)
		case proto.CRDTType_MVREG:
			downOp = DownstreamMVSetValue{}.FromReplicatorObj(protobuf)
		case proto.CRDTType_SIMPLE_DATE:
			downOp = downstreamProtoSimpleDateToAntidoteDownstream(protobuf)
		case proto.CRDTType_SETW_DATE:
			downOp = downstreamProtoSetWDateToAntidoteDownstream(protobuf)
		case proto.CRDTType_INCW_DATE:
			downOp = downstreamProtoIncWDateToAntidoteDownstream(protobuf)
		case proto.CRDTType_SET_ONLY_DATE:
			downOp = DownstreamSetTsSetOnly{}.FromReplicatorObj(protobuf)
		case proto.CRDTType_ARRAY_COMPACT:
			downOp = downstreamProtoCompactArrayToAntidoteDownstream(protobuf)
		case proto.CRDTType_ARRAY_STRING:
			downOp = downstreamProtoStringArrayToAntidoteDownstream(protobuf)
		case proto.CRDTType_MAP_COUNTER:
			downOp = downstreamProtoMapCounterToAntidoteDownstream(protobuf)
		case proto.CRDTType_TOPK_RMV_EXT:
			downOp = downstreamProtoTopKRmvExtToAntidoteDownstream(protobuf)
		case proto.CRDTType_ARRAY_BYTE:
			downOp = downstreamProtoByteArrayToAntidoteDownstream(protobuf)
		}
	} else if specialOp == proto.SPECIAL_UPD_RESET {
		downOp = ResetOp{}
	} else if specialOp == proto.SPECIAL_UPD_MULTI {
		downOp = MultiUpd{}.FromReplicatorObj(protobuf)
	} //No further cases.

	return
}

func StateProtoToCrdt(protobuf *proto.ProtoState, crdtType proto.CRDTType, ts *clocksi.Timestamp, replicaID uint16) (crdt CRDT) {
	switch crdtType {
	case proto.CRDTType_COUNTER:
		crdt = (&CounterCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_LWWREG:
		crdt = (&LwwRegisterCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_COUNTER_FLOAT:
		crdt = (&CounterFloatCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_ORSET:
		crdt = (&SetAWCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_ORMAP:
		crdt = (&ORMapCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_RRMAP:
		crdt = (&RWEmbMapCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_TOPK_RMV:
		crdt = (&TopKRmvCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_AVG:
		crdt = (&AvgCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_MAXMIN:
		crdt = (&MaxMinCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_TOPSUM:
		crdt = (&TopSumCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_TOPK:
		crdt = (&TopKCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_FLAG_EW:
		crdt = (&EwFlagCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_FLAG_DW:
		crdt = (&DwFlagCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_FLAG_LWW:
		crdt = (&LwwFlagCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_FATCOUNTER:
		crdt = (&BoundedCounterCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_PAIR_COUNTER:
		crdt = (&PairCounterCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_ARRAY_COUNTER:
		crdt = (&CounterArrayCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_ARRAY_FLOAT:
		crdt = (&FloatArrayCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_MULTI_ARRAY:
		crdt = (&MultiArrayCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_MVREG:
		crdt = (&MVRegisterCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_SIMPLE_DATE:
		crdt = (&SimpleDateCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_SETW_DATE:
		crdt = (&SetWDateCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_INCW_DATE:
		crdt = (&IncWDateCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_SET_ONLY_DATE:
		crdt = (&SetOnlyDateCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_ARRAY_COMPACT:
		crdt = (&CompactArrayCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_ARRAY_STRING:
		crdt = (&StringArrayCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_MAP_COUNTER:
		crdt = mapCounterToProtoState(protobuf, ts, replicaID)
	}
	return
}

func CrdtToProtoCRDT(keyHash uint64, crdt CRDT) (protobuf *proto.ProtoCRDT) {
	protoState, crdtType := crdt.(ProtoCRDT).ToProtoState(), crdt.GetCRDTType()
	return &proto.ProtoCRDT{KeyHash: &keyHash, Type: &crdtType, State: protoState}
}

/*****GLOBAL HELPER FUNCS*****/
/***SELECTION HELPERS***/

func updateSetProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	updType := protobuf.GetSetop().GetOptype()
	if updType == proto.ApbSetUpdate_ADD {
		return AddAll{}.FromUpdateObject(protobuf)
	} else {
		return RemoveAll{}.FromUpdateObject(protobuf)
	}
}

func updateMapProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if len(protobuf.GetMapop().GetUpdates()) > 0 {
		return MapAddAll{}.FromUpdateObject(protobuf)
	}
	return MapRemoveAll{}.FromUpdateObject(protobuf)
}

func updateEmbMapProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	mapOp := protobuf.GetMapop()
	if nUpds := len(mapOp.GetUpdates()); nUpds > 0 {
		if mapOp.GetIsAddsArray() {
			return EmbMapUpdateAllArray{}.FromUpdateObject(protobuf)
		} else if nUpds > 1 {
			return EmbMapUpdateAll{}.FromUpdateObject(protobuf)
		}
		return EmbMapUpdate{}.FromUpdateObject(protobuf)
	} else if nUpds := len(mapOp.GetRemovedKeys()); nUpds > 0 {
		if nUpds > 1 {
			return MapRemoveAll{}.FromUpdateObject(protobuf)
		}
		return MapRemove{}.FromUpdateObject(protobuf)
	}
	return EmbMapInit{}.FromUpdateObject(protobuf)
}

func updateTopkRmvProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if topKInit := protobuf.GetTopkinitop(); topKInit != nil {
		switch topKInit.GetTopType() {
		case proto.CRDTType_TOPK_RMV:
			return TopKRmvInit{}.FromUpdateObject(protobuf)
		case proto.CRDTType_TOPSUM:
			return TopSInit(0).FromUpdateObject(protobuf)
		case proto.CRDTType_TOPK:
			return TopKInit(0).FromUpdateObject(protobuf)
		case proto.CRDTType_TOPK_RMV_EXT:
			return TopKRmvExtTopInit{}.FromUpdateObject(protobuf)
		}
	}
	if adds := protobuf.GetTopkrmvop().GetAdds(); len(adds) > 0 {
		if len(adds) == 1 {
			return TopKAdd{}.FromUpdateObject(protobuf)
		}
		return TopKAddAll{}.FromUpdateObject(protobuf)
	}
	if len(protobuf.GetTopkrmvop().GetRems()) == 1 {
		return TopKRemove{}.FromUpdateObject(protobuf)
	}
	return TopKRemoveAll{}.FromUpdateObject(protobuf)
}

func updateTopsProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if protobuf.GetTopkinitop() != nil {
		return TopSInit(0).FromUpdateObject(protobuf)
	}
	topKProto := protobuf.GetTopkrmvop()
	adds := topKProto.GetAdds()
	if len(adds) == 1 {
		if adds[0].GetScore() >= 0 {
			return TopSAdd{}.FromUpdateObject(protobuf)
		}
		return TopSSub{}.FromUpdateObject(protobuf)
	} /*else if len(adds) >= 0 {
		return TopSAddAll{}.FromUpdateObject(protobuf)
	}
	return TopSSubAll{}.FromUpdateObject(protobuf)*/
	nPositive := int(topKProto.GetPositiveLen())
	if nPositive == len(adds) {
		return TopSAddAll{}.FromUpdateObject(protobuf)
	}
	if nPositive == 0 {
		return TopSSubAll{}.FromUpdateObject(protobuf)
	}
	return TopSAddAndSubAll{}.FromUpdateObject(protobuf)
}

func updateTopkProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if protobuf.GetTopkinitop() != nil {
		return TopKInit(0).FromUpdateObject(protobuf)
	}
	adds := protobuf.GetTopkrmvop().GetAdds()
	if len(adds) == 1 {
		return TopKAdd{}.FromUpdateObject(protobuf)
	}
	return TopKAddAll{}.FromUpdateObject(protobuf)
}

func updateMaxMinProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if protobuf.GetMaxminop().GetIsMax() {
		return MaxAddValue{}.FromUpdateObject(protobuf)
	}
	return MinAddValue{}.FromUpdateObject(protobuf)
}

func updateFlagProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	flag := protobuf.GetFlagop().GetValue()
	if flag {
		return EnableFlag{}
	}
	return DisableFlag{}
}

func updateBCounterProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	counter := protobuf.GetCounterop()
	if value := counter.GetInc(); value != 0 {
		if value > 0 {
			return Increment{}.FromUpdateObject(protobuf)
		} else {
			return Decrement{}.FromUpdateObject(protobuf)
		}
	}
	return SetCounterBound{}.FromUpdateObject(protobuf)
}

func updatePairCounterProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	pairUpd := protobuf.GetPaircounterop()
	incFirst, incSecond := pairUpd.GetIncFirst(), pairUpd.GetIncSecond()
	if incFirst != 0 && incSecond != 0 {
		if incFirst < 0 && incSecond < 0 {
			return DecrementBoth{}.FromUpdateObject(protobuf)
		}
		return IncrementBoth{}.FromUpdateObject(protobuf)
	} else if incFirst != 0 {
		if incFirst < 0 {
			return DecrementFirst(0).FromUpdateObject(protobuf)
		}
		return IncrementFirst(0).FromUpdateObject(protobuf)
	} else { //incSecond != 0
		if incSecond < 0 {
			return DecrementSecond(0).FromUpdateObject(protobuf)
		}
		return IncrementSecond(0).FromUpdateObject(protobuf)
	}
}

func updateArrayCounterProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayCounter := protobuf.GetArraycounterop()
	//fmt.Printf("[CRDTProtoLib]updateArrayCounterProtoToAntidoteUpdate. Protobuf: %+v\n", arrayCounter)
	switch typedUpd := arrayCounter.Upd.(type) {
	case *proto.ApbArrayCounterUpdate_Inc:
		if typedUpd.Inc.GetInc() >= 0 {
			return CounterArrayIncrement{}.FromUpdateObject(protobuf)
		}
		return CounterArrayDecrement{}.FromUpdateObject(protobuf)
	case *proto.ApbArrayCounterUpdate_IncAll:
		if typedUpd.IncAll.GetInc() >= 0 {
			return CounterArrayIncrementAll(0).FromUpdateObject(protobuf)
		}
		return CounterArrayDecrementAll(0).FromUpdateObject(protobuf)
	case *proto.ApbArrayCounterUpdate_IncMulti:
		if typedUpd.IncMulti.GetIncs()[0] >= 0 {
			return CounterArrayIncrementMulti([]int64{}).FromUpdateObject(protobuf)
		}
		return CounterArrayDecrementMulti([]int64{}).FromUpdateObject(protobuf)
	case *proto.ApbArrayCounterUpdate_IncSub:
		if typedUpd.IncSub.GetIncs()[0] >= 0 {
			return CounterArrayIncrementSub{}.FromUpdateObject(protobuf)
		}
		return CounterArrayDecrementSub{}.FromUpdateObject(protobuf)
	case *proto.ApbArrayCounterUpdate_Size:
		return CounterArraySetSize(0).FromUpdateObject(protobuf)
	}
	/*if inc := arrayCounter.GetInc(); inc != nil {
		if inc.GetInc() >= 0 {
			return CounterArrayIncrement{}.FromUpdateObject(protobuf)
		}
		return CounterArrayDecrement{}.FromUpdateObject(protobuf)
	} else if incAll := arrayCounter.GetIncAll(); incAll != nil {
		if incAll.GetInc() >= 0 {
			return CounterArrayIncrementAll(0).FromUpdateObject(protobuf)
		}
		return CounterArrayDecrementAll(0).FromUpdateObject(protobuf)
	} else if incMult := arrayCounter.GetIncMulti(); incMult != nil {
		if incMult.GetIncs()[0] >= 0 {
			return CounterArrayIncrementMulti([]int64{}).FromUpdateObject(protobuf)
		}
		return CounterArrayDecrementMulti([]int64{}).FromUpdateObject(protobuf)
	} else if incSub := arrayCounter.GetIncSub(); incSub != nil {
		if incSub.GetIncs()[0] >= 0 {
			return CounterArrayIncrementSub{}.FromUpdateObject(protobuf)
		}
		return CounterArrayDecrementSub{}.FromUpdateObject(protobuf)
	}
	return CounterArraySetSize(0).FromUpdateObject(protobuf)*/
	//fmt.Printf("[CRDTProtoLib][WARNING]updateArrayCounterProtoToAntidoteUpdate. Didn't match update!.\n")
	return nil
}

func updateArrayFloatProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayFloat := protobuf.GetArrayfloatop()
	switch typedUpd := arrayFloat.Upd.(type) {
	case *proto.ApbArrayFloatUpdate_Inc:
		if typedUpd.Inc.GetInc() >= 0 {
			return FloatArrayIncrement{}.FromUpdateObject(protobuf)
		}
		return FloatArrayDecrement{}.FromUpdateObject(protobuf)
	case *proto.ApbArrayFloatUpdate_IncAll:
		if typedUpd.IncAll.GetInc() >= 0 {
			return FloatArrayIncrementAll(0).FromUpdateObject(protobuf)
		}
		return FloatArrayDecrementAll(0).FromUpdateObject(protobuf)
	case *proto.ApbArrayFloatUpdate_IncMulti:
		if typedUpd.IncMulti.GetIncs()[0] >= 0 {
			return FloatArrayIncrementMulti(nil).FromUpdateObject(protobuf)
		}
		return FloatArrayDecrementMulti(nil).FromUpdateObject(protobuf)
	case *proto.ApbArrayFloatUpdate_IncSub:
		if typedUpd.IncSub.GetIncs()[0] >= 0 {
			return FloatArrayIncrementSub{}.FromUpdateObject(protobuf)
		}
		return FloatArrayDecrementSub{}.FromUpdateObject(protobuf)
	case *proto.ApbArrayFloatUpdate_IncRange:
		if typedUpd.IncRange.GetInc() >= 0 {
			return FloatArrayIncrementRange{}.FromUpdateObject(protobuf)
		}
		return FloatArrayDecrementRange{}.FromUpdateObject(protobuf)
	case *proto.ApbArrayFloatUpdate_Size:
		return FloatArraySetSize(0).FromUpdateObject(protobuf)
	}
	return nil
	/*if inc := arrayFloat.GetInc(); inc != nil {
		if inc.GetInc() >= 0 {
			return FloatArrayIncrement{}.FromUpdateObject(protobuf)
		}
		return FloatArrayDecrement{}.FromUpdateObject(protobuf)
	} else if incAll := arrayFloat.GetIncAll(); incAll != nil {
		if incAll.GetInc() >= 0 {
			return FloatArrayIncrementAll(0).FromUpdateObject(protobuf)
		}
		return FloatArrayDecrementAll(0).FromUpdateObject(protobuf)
	} else if incMult := arrayFloat.GetIncMulti(); incMult != nil {
		if incMult.GetIncs()[0] >= 0 {
			return FloatArrayIncrementMulti([]float64{}).FromUpdateObject(protobuf)
		}
		return FloatArrayDecrementMulti([]float64{}).FromUpdateObject(protobuf)
	} else if incSub := arrayFloat.GetIncSub(); incSub != nil {
		if incSub.GetIncs()[0] >= 0 {
			return FloatArrayIncrementSub{}.FromUpdateObject(protobuf)
		}
		return FloatArrayDecrementSub{}.FromUpdateObject(protobuf)
	}
	return FloatArraySetSize(0).FromUpdateObject(protobuf)*/
}

func updateMultiArrayProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	multiArray := protobuf.GetMultiarrayop()
	arrayType := multiArray.GetType()
	//fmt.Printf("[CRDTProtoLib]Multi array update proto to antidote. Start. ArrayType: %+v.\n", arrayType)
	switch arrayType {
	case proto.MultiArrayType_MA_INT:
		counterUpd := multiArray.GetIntUpd() //Inc, Multi, Sub
		switch counterUpd.Upd.(type) {
		case *proto.ApbMultiArrayIntUpdate_IncSingle:
			return MultiArrayIncIntSingle{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayIntUpdate_Inc:
			return MultiArrayIncInt(nil).FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayIntUpdate_IncPos:
			return MultiArrayIncIntPositions{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayIntUpdate_IncRange:
			return MultiArrayIncIntRange{}.FromUpdateObject(protobuf)
		}
		/*if inc := counterUpd.GetIncSingle(); inc != nil {
			return MultiArrayIncIntSingle{}.FromUpdateObject(protobuf)
		}
		if incMult := counterUpd.GetInc(); incMult != nil {
			return MultiArrayIncInt([]int64{}).FromUpdateObject(protobuf)
		}
		if incSub := counterUpd.GetIncPos(); incSub != nil {
			return MultiArrayIncIntPositions{}.FromUpdateObject(protobuf)
		}
		if incRange := counterUpd.GetIncRange(); incRange != nil {
			return MultiArrayIncIntRange{}.FromUpdateObject(protobuf)
		}*/
	case proto.MultiArrayType_MA_FLOAT:
		floatUpd := multiArray.GetFloatUpd()
		switch floatUpd.Upd.(type) {
		case *proto.ApbMultiArrayFloatUpdate_IncSingle:
			return MultiArrayIncFloatSingle{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayFloatUpdate_Inc:
			return MultiArrayIncFloat(nil).FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayFloatUpdate_IncPos:
			return MultiArrayIncFloatPositions{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayFloatUpdate_IncRange:
			return MultiArrayIncFloatRange{}.FromUpdateObject(protobuf)
		}
		/*if inc := floatUpd.GetIncSingle(); inc != nil {
			return MultiArrayIncFloatSingle{}.FromUpdateObject(protobuf)
		}
		if incMult := floatUpd.GetInc(); incMult != nil {
			return MultiArrayIncFloat([]float64{}).FromUpdateObject(protobuf)
		}
		if incSub := floatUpd.GetIncPos(); incSub != nil {
			return MultiArrayIncFloatPositions{}.FromUpdateObject(protobuf)
		}
		if incRange := floatUpd.GetIncRange(); incRange != nil {
			return MultiArrayIncFloatRange{}.FromUpdateObject(protobuf)
		}*/
	case proto.MultiArrayType_MA_DATA:
		dataUpd := multiArray.GetDataUpd()
		switch dataUpd.Upd.(type) {
		case *proto.ApbMultiArrayDataUpdate_SetSingle:
			return MultiArraySetRegisterSingle{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayDataUpdate_Set:
			return MultiArraySetRegister(nil).FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayDataUpdate_SetPos:
			return MultiArraySetRegisterPositions{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayDataUpdate_SetRange:
			return MultiArraySetRegisterRange{}.FromUpdateObject(protobuf)
		}
		/*if inc := dataUpd.GetSetSingle(); inc != nil {
			return MultiArraySetRegisterSingle{}.FromUpdateObject(protobuf)
		}
		if incMult := dataUpd.GetSet(); incMult != nil {
			return MultiArraySetRegister([][]byte{}).FromUpdateObject(protobuf)
		}
		if incSub := dataUpd.GetSetPos(); incSub != nil {
			return MultiArraySetRegisterPositions{}.FromUpdateObject(protobuf)
		}
		if incRange := dataUpd.GetSetRange(); incRange != nil {
			return MultiArraySetRegisterRange{}.FromUpdateObject(protobuf)
		}*/
	case proto.MultiArrayType_MA_AVG:
		avgUpd := multiArray.GetAvgUpd()
		switch avgUpd.Upd.(type) {
		case *proto.ApbMultiArrayAvgUpdate_IncSingle:
			return MultiArrayIncAvgSingle{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayAvgUpdate_Inc:
			return MultiArrayIncAvg{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayAvgUpdate_IncPos:
			return MultiArrayIncAvgPositions{}.FromUpdateObject(protobuf)
		case *proto.ApbMultiArrayAvgUpdate_IncRange:
			return MultiArrayIncAvgRange{}.FromUpdateObject(protobuf)
		}
		/*if inc := avgUpd.GetIncSingle(); inc != nil {
			return MultiArrayIncAvgSingle{}.FromUpdateObject(protobuf)
		}
		if incMult := avgUpd.GetInc(); incMult != nil {
			return MultiArrayIncAvg{}.FromUpdateObject(protobuf)
		}
		if incSub := avgUpd.GetIncPos(); incSub != nil {
			return MultiArrayIncAvgPositions{}.FromUpdateObject(protobuf)
		}
		if incRange := avgUpd.GetIncRange(); incRange != nil {
			return MultiArrayIncAvgRange{}.FromUpdateObject(protobuf)
		}*/
	case proto.MultiArrayType_MA_MULTI:
		return MultiArrayUpdateAll{}.FromUpdateObject(protobuf)
	case proto.MultiArrayType_MA_SIZE:
		return MultiArraySetSizes{}.FromUpdateObject(protobuf)
	default:
		fmt.Printf("[CRDTProtoLib][ERROR]Unknown type of multi array update. ArrayType: %+v.\n", arrayType)
	}
	//fmt.Printf("[CRDTProtoLib][ERROR]Did not match update. ArrayType: %+v.\n. MultiArrayProto: %+v\n", arrayType, protobuf)
	return nil
}

func updateArrayByteProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayBytes := protobuf.GetBytearrayop()
	switch arrayBytes.Upd.(type) {
	case *proto.ApbByteArrayUpdate_SetValue:
		return ByteArraySetValue{}.FromUpdateObject(protobuf)
	case *proto.ApbByteArrayUpdate_SetData:
		return ByteArraySetData{}.FromUpdateObject(protobuf)
	case *proto.ApbByteArrayUpdate_IntInc:
		return ByteArrayIncrement{}.FromUpdateObject(protobuf)
	case *proto.ApbByteArrayUpdate_FloatInc:
		return ByteArrayFloatInc{}.FromUpdateObject(protobuf)
	case *proto.ApbByteArrayUpdate_SetDataInit:
		return ByteArraySetDataInitialize{}.FromUpdateObject(protobuf)
	}
	return nil
}

func updateDateProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation, crdtType proto.CRDTType) (op UpdateArguments) {
	//Note: This could be optimized by doing a switch on dateOp.Upd, similarly to above.
	dateOp := protobuf.GetDateop()
	if set := dateOp.GetSet(); set != nil {
		if set.Millisecond != nil {
			return SetDateFull{}.FromUpdateObject(protobuf)
		}
		return SetDate{}.FromUpdateObject(protobuf)
	} else if dateOnly := dateOp.GetDateSet(); dateOnly != nil {
		return SetDateOnly{}.FromUpdateObject(protobuf)
	} else if timeOnly := dateOp.GetTimeSet(); timeOnly != nil {
		return SetTime{}.FromUpdateObject(protobuf)
	} else if setMs := dateOp.GetSetMS(); setMs != nil {
		switch crdtType {
		case proto.CRDTType_SIMPLE_DATE:
			return SetMSSimple(0).FromUpdateObject(protobuf)
		case proto.CRDTType_SETW_DATE:
			return SetMSSetW(0).FromUpdateObject(protobuf)
		case proto.CRDTType_INCW_DATE:
			return SetMSIncW(0).FromUpdateObject(protobuf)
		case proto.CRDTType_SET_ONLY_DATE:
			return SetMSSetOnly(0).FromUpdateObject(protobuf)
		}
	}
	if crdtType != proto.CRDTType_SET_ONLY_DATE { //All of these are not supported by SET_ONLY
		if inc := dateOp.GetInc(); inc != nil {
			if inc.Millisecond != nil {
				return IncDateFull{}.FromUpdateObject(protobuf)
			}
			return IncDate{}.FromUpdateObject(protobuf)
		} else if dateOnlyInc := dateOp.GetDateInc(); dateOnlyInc != nil {
			return IncDateOnly{}.FromUpdateObject(protobuf) //TODO: Check
		} else if timeOnlyInc := dateOp.GetTimeInc(); timeOnlyInc != nil { //TODO: Check
			return IncTime{}.FromUpdateObject(protobuf) //TODO: Define structs for each of these. It's OK.
		} else if incMs := dateOp.GetIncMS(); incMs != nil {
			switch crdtType {
			case proto.CRDTType_SIMPLE_DATE:
				return IncMSSimple(0).FromUpdateObject(protobuf)
			case proto.CRDTType_SETW_DATE:
				return IncMSSetW(0).FromUpdateObject(protobuf)
			case proto.CRDTType_INCW_DATE:
				return IncMSIncW(0).FromUpdateObject(protobuf)
				//Set_only does not support this kind of update.
			}
		} else if initialize := dateOp.GetInitialize(); initialize != nil {
			if crdtType == proto.CRDTType_SIMPLE_DATE {
				return SetInitialDate{}.FromUpdateObject(protobuf)
			}
		}
	}
	return nil
}

func updateCompactArrayProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayOp := protobuf.GetCompactarrayop()
	if setValue := arrayOp.GetSetValue(); setValue != nil {
		return CompactArraySetValue{}.FromUpdateObject(protobuf)
	} else if intInc := arrayOp.GetIntInc(); intInc != nil {
		if intInc.GetInc() >= 0 {
			return CompactArrayIncrement{}.FromUpdateObject(protobuf)
		}
		return CompactArrayDecrement{}.FromUpdateObject(protobuf)
	} else if floatInc := arrayOp.GetFloatInc(); floatInc != nil {
		if intInc.GetInc() >= 0 {
			return CompactArrayFloatInc{}.FromUpdateObject(protobuf)
		}
		return CompactArrayFloatDec{}.FromUpdateObject(protobuf)
	} else if setArray := arrayOp.GetSetArray(); setArray != nil {
		return CompactArraySetArray{}.FromUpdateObject(protobuf)
	} else { //SetSize
		return CompactArraySetSize(0).FromUpdateObject(protobuf)
	}
}

func updateStringArrayProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayOp := protobuf.GetStringarrayop()
	if setValue := arrayOp.GetSetValue(); setValue != nil {
		return StringArraySetValue{}.FromUpdateObject(protobuf)
	} else if setArray := arrayOp.GetSetArray(); setArray != nil {
		return StringArraySetArray{}.FromUpdateObject(protobuf)
	} else if setArrayInit := arrayOp.GetSetArrayInit(); setArrayInit != nil {
		return StringArraySetArrayInitialize{}.FromUpdateObject(protobuf)
	} else { //SetSize
		return StringArraySetSize(0).FromUpdateObject(protobuf)
	}
}

func updateMapCounterProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	mapOp := protobuf.GetMapcounterop()
	dataType, isDec := mapOp.GetDataType(), mapOp.GetIsDec()
	if intOp := mapOp.GetIntOp(); intOp != nil {
		if intOp.GetInc() != nil {
			return apbMapSingleIncOpToUpdateObject(protobuf, isDec, dataType)
		} else if incAllOp := intOp.GetIncAll(); incAllOp != nil {
			return apbMapIncAllOpToUpdateObject(protobuf, isDec, dataType)
		} else {
			return apbMapIncMultOpToUpdateObject(protobuf, isDec, dataType)
		}
	} else if floatOp := mapOp.GetDoubleOp(); floatOp != nil {
		if floatOp.GetInc() != nil {
			return apbMapSingleIncOpToUpdateObject(protobuf, isDec, dataType)
		} else if floatOp.GetIncAll() != nil {
			return apbMapIncAllOpToUpdateObject(protobuf, isDec, dataType)
		} else {
			return apbMapIncMultOpToUpdateObject(protobuf, isDec, dataType)
		}
	} else {
		return apbMapCounterInitOpToUpdateObject(protobuf, dataType)
	}
}

func apbMapSingleIncOpToUpdateObject(protobuf *proto.ApbUpdateOperation, isDec bool, dataType proto.DATAType) UpdateArguments {
	if !isDec {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapInc[int]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT64:
			return CounterMapInc[int64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT32:
			return CounterMapInc[int32]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT16:
			return CounterMapInc[int16]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT8:
			return CounterMapInc[int8]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapInc[float64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapInc[float32]{}.FromUpdateObject(protobuf)
		}
	} else {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapDec[int]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT64:
			return CounterMapDec[int64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT32:
			return CounterMapDec[int32]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT16:
			return CounterMapDec[int16]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT8:
			return CounterMapDec[int8]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapDec[float64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapDec[float32]{}.FromUpdateObject(protobuf)
		}
	}
	return nil
}

func apbMapIncAllOpToUpdateObject(protobuf *proto.ApbUpdateOperation, isDec bool, dataType proto.DATAType) UpdateArguments {
	if !isDec {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapIncAll[int]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT64:
			return CounterMapIncAll[int64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT32:
			return CounterMapIncAll[int32]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT16:
			return CounterMapIncAll[int16]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT8:
			return CounterMapIncAll[int8]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapIncAll[float64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapIncAll[float32]{}.FromUpdateObject(protobuf)
		}
	} else {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapDecAll[int]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT64:
			return CounterMapDecAll[int64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT32:
			return CounterMapDecAll[int32]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT16:
			return CounterMapDecAll[int16]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT8:
			return CounterMapDecAll[int8]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapDecAll[float64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapDecAll[float32]{}.FromUpdateObject(protobuf)
		}
	}
	return nil
}

func apbMapIncMultOpToUpdateObject(protobuf *proto.ApbUpdateOperation, isDec bool, dataType proto.DATAType) UpdateArguments {
	if !isDec {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapIncMult[int]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT64:
			return CounterMapIncMult[int64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT32:
			return CounterMapIncMult[int32]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT16:
			return CounterMapIncMult[int16]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT8:
			return CounterMapIncMult[int8]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapIncMult[float64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapIncMult[float32]{}.FromUpdateObject(protobuf)
		}
	} else {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapDecMult[int]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT64:
			return CounterMapDecMult[int64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT32:
			return CounterMapDecMult[int32]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT16:
			return CounterMapDecMult[int16]{}.FromUpdateObject(protobuf)
		case proto.DATAType_INT8:
			return CounterMapDecMult[int8]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapDecMult[float64]{}.FromUpdateObject(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapDecMult[float32]{}.FromUpdateObject(protobuf)
		}
	}
	return nil
}

func apbMapCounterInitOpToUpdateObject(protobuf *proto.ApbUpdateOperation, dataType proto.DATAType) UpdateArguments {
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapInit[int](0).FromUpdateObject(protobuf)
	case proto.DATAType_INT64:
		return CounterMapInit[int64](0).FromUpdateObject(protobuf)
	case proto.DATAType_INT32:
		return CounterMapInit[int32](0).FromUpdateObject(protobuf)
	case proto.DATAType_INT16:
		return CounterMapInit[int16](0).FromUpdateObject(protobuf)
	case proto.DATAType_INT8:
		return CounterMapInit[int8](0).FromUpdateObject(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapInit[float64](0).FromUpdateObject(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapInit[float32](0).FromUpdateObject(protobuf)
	}
	return nil
}

/*


func apbMapIntSingleIncOpToUpdateObject(protobf *proto.ApbMapIntSingleIncOp, isDec bool, dataType proto.DATAType) UpdateArguments {
	if !isDec {
		switch dataType {
		case proto.DATAType_INT:

		case proto.DATAType_INT64:

		case proto.DATAType_INT32:

		case proto.DATAType_INT16:

		case proto.DATAType_INT8:

		}
	} else {
		switch dataType {
		case proto.DATAType_FLOAT64:

		case proto.DATAType_FLOAT32:

		}
	}
	return nil
}
*/

//Read Resps

func partialHasKeyRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType) (state State) {
	if crdtType == proto.CRDTType_ORMAP {
		return MapKeysState{}.FromReadResp(protobuf)
	}
	return EmbMapKeysState{}.FromReadResp(protobuf)
}

func partialGetKeysRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType) (state State) {
	if crdtType == proto.CRDTType_ORMAP {
		return MapKeysState{}.FromReadResp(protobuf)
	}
	return EmbMapKeysState{}.FromReadResp(protobuf)
}

func partialGetValueRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType) (state State) {
	if crdtType == proto.CRDTType_ORMAP {
		return MapGetValueState{}.FromReadResp(protobuf)
	}
	return EmbMapGetValueState{}.FromReadResp(protobuf)
}

func partialGetValuesRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType) (state State) {
	if crdtType == proto.CRDTType_ORMAP {
		return MapEntryState{}.FromReadResp(protobuf)
	}
	//return EmbMapEntryState{}.FromReadResp(protobuf)
	return EmbMapGetValuesState{}.FromReadResp(protobuf)
}

func partialGetAggregateRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	if floatState := protobuf.GetCounterfloat(); floatState != nil {
		return CounterFloatState(0.0).FromReadResp(protobuf)
	}
	return AvgFullState{}.FromReadResp(protobuf)
}

func partialTopRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType) (state State) {
	if crdtType == proto.CRDTType_TOPK_RMV {
		return TopKValueState{}.FromReadResp(protobuf)
	}
	return TopSValueState{}.FromReadResp(protobuf)
}

func partialTopAggrRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	if protobuf.GetPartread().GetTopk().GetAggr().Count == nil {
		return TopAggrState(0).FromReadResp(protobuf)
	}
	return TopAggrAvgState{}.FromReadResp(protobuf)
}

func partialMultiDataCondRespToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	arrayType := protobuf.GetPartread().GetMultiarray().GetType()
	switch arrayType {
	case proto.MultiArrayType_MA_INT:
		return MultiArrayDataSliceIntPosState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_MA_FLOAT:
		return MultiArrayDataSliceFloatPosState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_MA_AVG:
		return MultiArrayDataSliceAvgPosState{}.FromReadResp(protobuf)
	}
	return nil
}

func partialMultiFullRespToAntidoteRead(protobuf *proto.ApbReadObjectResp) (state State) {
	arrayType := protobuf.GetPartread().GetMultiarray().GetType()
	switch arrayType {
	case proto.MultiArrayType_MA_INT:
		return IntArrayState(nil).FromReadResp(protobuf)
	case proto.MultiArrayType_MA_FLOAT:
		return FloatArrayState(nil).FromReadResp(protobuf)
	case proto.MultiArrayType_MA_AVG:
		return AvgArrayState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_MA_DATA:
		return DataArrayState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_MA_MULTI:
		return MultiArrayState{}.FromReadResp(protobuf)
	}
	return nil
}
func partialMultiSingleRespToAntidoteRead(protobuf *proto.ApbReadObjectResp) (state State) {
	arrayType := protobuf.GetPartread().GetMultiarray().GetType()
	switch arrayType {
	case proto.MultiArrayType_MA_INT:
		return IntArraySingleState(0).FromReadResp(protobuf)
	case proto.MultiArrayType_MA_FLOAT:
		return FloatArraySingleState(0).FromReadResp(protobuf)
	case proto.MultiArrayType_MA_AVG:
		return AvgArraySingleState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_MA_DATA:
		return DataArraySingleState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_MA_MULTI:
		return MultiArraySingleState{}.FromReadResp(protobuf)
	}
	return nil
}

func partialCompactArrayRespToAntidoteRead(protobuf *proto.ApbReadObjectResp) (state State) {
	arrayProto := protobuf.GetPartread().GetCompactarray()
	if anyProto := arrayProto.GetAnyValue(); anyProto != nil {
		return CompactArraySingleAny{}.FromReadResp(protobuf)
	} else if intProto := arrayProto.GetIntValue(); intProto != nil {
		return CompactArraySingleCounter(0).FromReadResp(protobuf)
	} else if floatProto := arrayProto.GetFloatValue(); floatProto != nil {
		return CompactArraySingleFloat(0.0).FromReadResp(protobuf)
	} else if dataProto := arrayProto.GetDataValue(); dataProto != nil {
		return CompactArraySingleData{}.FromReadResp(protobuf)
	} else { //String
		return CompactArraySingleString("").FromReadResp(protobuf)
	}
}

func stateMapCounterProtoToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	dataType := protobuf.GetMapcounter().GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapState[int]{}.FromReadResp(protobuf)
	case proto.DATAType_INT64:
		return CounterMapState[int64]{}.FromReadResp(protobuf)
	case proto.DATAType_INT32:
		return CounterMapState[int32]{}.FromReadResp(protobuf)
	case proto.DATAType_INT16:
		return CounterMapState[int16]{}.FromReadResp(protobuf)
	case proto.DATAType_INT8:
		return CounterMapState[int8]{}.FromReadResp(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapState[float64]{}.FromReadResp(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapState[float32]{}.FromReadResp(protobuf)
	}
	return
}

func partialCounterMapValueRespToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	dataType := protobuf.GetPartread().GetMapcounter().GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapSingleState[int]{}.FromReadResp(protobuf)
	case proto.DATAType_INT64:
		return CounterMapSingleState[int64]{}.FromReadResp(protobuf)
	case proto.DATAType_INT32:
		return CounterMapSingleState[int32]{}.FromReadResp(protobuf)
	case proto.DATAType_INT16:
		return CounterMapSingleState[int16]{}.FromReadResp(protobuf)
	case proto.DATAType_INT8:
		return CounterMapSingleState[int8]{}.FromReadResp(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapSingleState[float64]{}.FromReadResp(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapSingleState[float32]{}.FromReadResp(protobuf)
	}
	return
}

func partialCounterMapKeysRespToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	dataType := protobuf.GetPartread().GetMapcounter().GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapKeysState[int]{}.FromReadResp(protobuf)
	case proto.DATAType_INT64:
		return CounterMapKeysState[int64]{}.FromReadResp(protobuf)
	case proto.DATAType_INT32:
		return CounterMapKeysState[int32]{}.FromReadResp(protobuf)
	case proto.DATAType_INT16:
		return CounterMapKeysState[int16]{}.FromReadResp(protobuf)
	case proto.DATAType_INT8:
		return CounterMapKeysState[int8]{}.FromReadResp(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapKeysState[float64]{}.FromReadResp(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapKeysState[float32]{}.FromReadResp(protobuf)
	}
	return
}

func partialCounterMapHasKeyRespToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	dataType := protobuf.GetPartread().GetMapcounter().GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapHasKeyState[int](true).FromReadResp(protobuf)
	case proto.DATAType_INT64:
		return CounterMapHasKeyState[int64](true).FromReadResp(protobuf)
	case proto.DATAType_INT32:
		return CounterMapHasKeyState[int32](true).FromReadResp(protobuf)
	case proto.DATAType_INT16:
		return CounterMapHasKeyState[int16](true).FromReadResp(protobuf)
	case proto.DATAType_INT8:
		return CounterMapHasKeyState[int8](true).FromReadResp(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapHasKeyState[float64](true).FromReadResp(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapHasKeyState[float32](true).FromReadResp(protobuf)
	}
	return
}

func partialCounterMapCompRespToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	mapProto := protobuf.GetPartread().GetMapcounter()
	dataType := mapProto.GetDataType()
	if mapProto.GetKeysData() != nil {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapKeysDataState[int]{}.FromReadResp(protobuf)
		case proto.DATAType_INT64:
			return CounterMapKeysDataState[int64]{}.FromReadResp(protobuf)
		case proto.DATAType_INT32:
			return CounterMapKeysDataState[int32]{}.FromReadResp(protobuf)
		case proto.DATAType_INT16:
			return CounterMapKeysDataState[int16]{}.FromReadResp(protobuf)
		case proto.DATAType_INT8:
			return CounterMapKeysDataState[int8]{}.FromReadResp(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapKeysDataState[float64]{}.FromReadResp(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapKeysDataState[float32]{}.FromReadResp(protobuf)
		}
	} else {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapValuesDataState[int]{}.FromReadResp(protobuf)
		case proto.DATAType_INT64:
			return CounterMapValuesDataState[int64]{}.FromReadResp(protobuf)
		case proto.DATAType_INT32:
			return CounterMapValuesDataState[int32]{}.FromReadResp(protobuf)
		case proto.DATAType_INT16:
			return CounterMapValuesDataState[int16]{}.FromReadResp(protobuf)
		case proto.DATAType_INT8:
			return CounterMapValuesDataState[int8]{}.FromReadResp(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapValuesDataState[float64]{}.FromReadResp(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapValuesDataState[float32]{}.FromReadResp(protobuf)
		}
	}
	return
}

func partialGetValueOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs, crdtType proto.CRDTType) (readArgs ReadArguments) {
	if crdtType == proto.CRDTType_ORMAP {
		return GetValueArguments{}.FromPartialRead(protobuf)
	}
	return EmbMapGetValueArguments{}.FromPartialRead(protobuf)
}

func partialGetValuesOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs, crdtType proto.CRDTType) (readArgs ReadArguments) {
	/*if crdtType == proto.CRDTType_ORMAP {
		return GetValuesArguments{}.FromPartialRead(protobuf)
	}
	return EmbMapPartialArguments{}.FromPartialRead(protobuf)
	*/
	if protobuf.GetMap().GetGetvalues().Args == nil {
		//if protobuf.GetMap().Getvalues.Args == nil {
		return GetValuesArguments{}.FromPartialRead(protobuf)
	}
	return EmbMapPartialArguments{}.FromPartialRead(protobuf)
}

func partialMultiDataCondOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	readType := protobuf.GetMultiarray().GetTypes()[0]
	switch readType {
	case proto.MultiArrayType_MA_INT:
		return MultiArrayDataIntComparableArguments{}.FromPartialRead(protobuf)
	case proto.MultiArrayType_MA_FLOAT:
		return MultiArrayDataFloatComparableArguments{}.FromPartialRead(protobuf)
	case proto.MultiArrayType_MA_AVG:
		return MultiArrayDataAvgComparableArguments{}.FromPartialRead(protobuf)
	}
	return nil
}

func partialMultiFullOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	if len(protobuf.GetMultiarray().GetTypes()) == 1 {
		return MultiArrayFullArguments(0).FromPartialRead(protobuf)
	}
	return MultiArrayFullTypesArguments{}.FromPartialRead(protobuf)
}

func partialMultiSingleOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	if len(protobuf.GetMultiarray().GetTypes()) == 1 {
		return MultiArrayPosArguments{}.FromPartialRead(protobuf)
	}
	return MultiArrayPosTypesArguments{}.FromPartialRead(protobuf)
}

func partialMultiRangeOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	if len(protobuf.GetMultiarray().GetTypes()) == 1 {
		return MultiArrayRangeArguments{}.FromPartialRead(protobuf)
	}
	return MultiArrayRangeTypesArguments{}.FromPartialRead(protobuf)
}

func partialMultiSubOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	if len(protobuf.GetMultiarray().GetTypes()) == 1 {
		return MultiArraySubArguments{}.FromPartialRead(protobuf)
	}
	return MultiArraySubTypesArguments{}.FromPartialRead(protobuf)
}

func partialCompactArrayPosToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	dataType := protobuf.GetCompactarray().GetPos().GetDataType()
	switch dataType {
	case proto.CA_Type_CA_ANY:
		return CompactArraySingleAnyArguments(0).FromPartialRead(protobuf)
	case proto.CA_Type_CA_DATA:
		return CompactArraySingleDataArguments(0).FromPartialRead(protobuf)
	case proto.CA_Type_CA_INT:
		return CompactArraySingleIntArguments(0).FromPartialRead(protobuf)
	case proto.CA_Type_CA_FLOAT:
		return CompactArraySingleFloatArguments(0).FromPartialRead(protobuf)
	case proto.CA_Type_CA_STRING:
		return CompactArraySingleStringArguments(0).FromPartialRead(protobuf)
	}
	return nil
}

func byteArrayPosToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	bytePb := protobuf.GetBytearray().GetPos()
	switch bytePb.GetDataType() {
	case proto.CA_Type_CA_ANY:
		return ByteArraySingleAnyArguments(0).FromPartialRead(protobuf)
	case proto.CA_Type_CA_DATA:
		return ByteArraySingleDataArguments(0).FromPartialRead(protobuf)
	case proto.CA_Type_CA_INT:
		return ByteArraySingleIntArguments(0).FromPartialRead(protobuf)
	case proto.CA_Type_CA_FLOAT:
		return ByteArraySingleFloatArguments(0).FromPartialRead(protobuf)
	case proto.CA_Type_CA_STRING:
		return ByteArraySingleStringArguments(0).FromPartialRead(protobuf)
	}
	return nil
}

func partialMapCounterValueToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	mapRead := protobuf.GetMapcounter()
	dataType := mapRead.GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapGetValueArguments[int](0).FromPartialRead(protobuf)
	case proto.DATAType_INT64:
		return CounterMapGetValueArguments[int64](0).FromPartialRead(protobuf)
	case proto.DATAType_INT32:
		return CounterMapGetValueArguments[int32](0).FromPartialRead(protobuf)
	case proto.DATAType_INT16:
		return CounterMapGetValueArguments[int16](0).FromPartialRead(protobuf)
	case proto.DATAType_INT8:
		return CounterMapGetValueArguments[int8](0).FromPartialRead(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapGetValueArguments[float64](0).FromPartialRead(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapGetValueArguments[float32](0).FromPartialRead(protobuf)
	}
	return nil
}

func partialMapCounterHasKeyToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	mapRead := protobuf.GetMapcounter()
	dataType := mapRead.GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapHasKeyArguments[int](0).FromPartialRead(protobuf)
	case proto.DATAType_INT64:
		return CounterMapHasKeyArguments[int64](0).FromPartialRead(protobuf)
	case proto.DATAType_INT32:
		return CounterMapHasKeyArguments[int32](0).FromPartialRead(protobuf)
	case proto.DATAType_INT16:
		return CounterMapHasKeyArguments[int16](0).FromPartialRead(protobuf)
	case proto.DATAType_INT8:
		return CounterMapHasKeyArguments[int8](0).FromPartialRead(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapHasKeyArguments[float64](0).FromPartialRead(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapHasKeyArguments[float32](0).FromPartialRead(protobuf)
	}
	return nil
}

func partialMapCounterKeysToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	mapRead := protobuf.GetMapcounter()
	dataType := mapRead.GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapGetKeysArguments[int]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT64:
		return CounterMapGetKeysArguments[int64]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT32:
		return CounterMapGetKeysArguments[int32]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT16:
		return CounterMapGetKeysArguments[int16]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT8:
		return CounterMapGetKeysArguments[int8]{}.FromPartialRead(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapGetKeysArguments[float64]{}.FromPartialRead(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapGetKeysArguments[float32]{}.FromPartialRead(protobuf)
	}
	return nil
}

func partialMapCounterValuesToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	mapRead := protobuf.GetMapcounter()
	dataType := mapRead.GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapGetValuesArguments[int]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT64:
		return CounterMapGetValuesArguments[int64]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT32:
		return CounterMapGetValuesArguments[int32]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT16:
		return CounterMapGetValuesArguments[int16]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT8:
		return CounterMapGetValuesArguments[int8]{}.FromPartialRead(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapGetValuesArguments[float64]{}.FromPartialRead(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapGetValuesArguments[float32]{}.FromPartialRead(protobuf)
	}
	return nil
}

func partialMapCounterCompToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	mapRead := protobuf.GetMapcounter()
	dataType := mapRead.GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapCompareAllArguments[int]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT64:
		return CounterMapCompareAllArguments[int64]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT32:
		return CounterMapCompareAllArguments[int32]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT16:
		return CounterMapCompareAllArguments[int16]{}.FromPartialRead(protobuf)
	case proto.DATAType_INT8:
		return CounterMapCompareAllArguments[int8]{}.FromPartialRead(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapCompareAllArguments[float64]{}.FromPartialRead(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapCompareAllArguments[float32]{}.FromPartialRead(protobuf)
	}
	return nil
}

func downstreamProtoCounterToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if protobuf.GetCounterOp().GetIsInc() {
		return Increment{}.FromReplicatorObj(protobuf)
	}
	return Decrement{}.FromReplicatorObj(protobuf)
}

func downstreamProtoCounterFloatToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if protobuf.GetCounterfloatOp().GetIsInc() {
		return IncrementFloat{}.FromReplicatorObj(protobuf)
	}
	return DecrementFloat{}.FromReplicatorObj(protobuf)
}

func downstreamProtoSetToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if adds := protobuf.GetSetOp().GetAdds(); adds != nil {
		return DownstreamAddAll{}.FromReplicatorObj(protobuf)
	}
	return DownstreamRemoveAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoORMapToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if adds := protobuf.GetOrmapOp().GetAdds(); adds != nil {
		return DownstreamORMapAddAll{}.FromReplicatorObj(protobuf)
	}
	return DownstreamORMapRemoveAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoRRMapToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	rwOp := protobuf.GetRwembmapOp()
	if adds := rwOp.GetAdds(); adds != nil {
		if len(adds.Upds) == 1 {
			return DownstreamRWEmbMapUpdateSingle{}.FromReplicatorObj(protobuf)
		}
		if !adds.GetIsArray() && !adds.GetIsFirstUpd() {
			return DownstreamRWEmbMapUpdateAll{}.FromReplicatorObj(protobuf)
		} else if !adds.GetIsFirstUpd() { //It's array.
			return DownstreamRWEmbMapUpdateAllArray{}.FromReplicatorObj(protobuf)
		}
		return RemoteDownstreamRWEmbMapFirstUpdate{}.FromReplicatorObj(protobuf)
	} else if rems := rwOp.GetRems().GetKeys(); rems != nil {
		if len(rems) > 1 {
			return DownstreamRWEmbMapRemoveAll{}.FromReplicatorObj(protobuf)
		}
		return DownstreamRWEmbMapRemoveSingle{}.FromReplicatorObj(protobuf)
	}
	return EmbMapInit{}.FromReplicatorObj(protobuf)
}

func downstreamProtoTopKRmvToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if protobuf.GetTopkinitOp() != nil {
		downOp = TopKRmvInit{}.FromReplicatorObj(protobuf)
		return downOp
	}
	if adds := protobuf.GetTopkrmvOp().GetAdds(); adds != nil {
		if len(adds) == 1 {
			return DownstreamTopKAdd{}.FromReplicatorObj(protobuf)
		} else {
			return DownstreamTopKAddAll{}.FromReplicatorObj(protobuf)
		}
	}
	if len(protobuf.GetTopkrmvOp().GetRems().GetIds()) == 1 {
		return DownstreamTopKRemove{}.FromReplicatorObj(protobuf)
	}
	return DownstreamTopKRemoveAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoTopKRmvExtToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if protobuf.GetTopkinitOp() != nil {
		downOp = TopKRmvExtTopInit{}.FromReplicatorObj(protobuf)
		return downOp
	}
	if adds := protobuf.GetTopkrmvOp().GetAdds(); adds != nil {
		if len(adds) == 1 {
			return DownstreamTopKRmvExtAdd{}.FromReplicatorObj(protobuf)
		} else {
			return DownstreamTopKRmvExtAddAll{}.FromReplicatorObj(protobuf)
		}
	}
	if len(protobuf.GetTopkrmvOp().GetRems().GetIds()) == 1 {
		return DownstreamTopKRmvExtRem{}.FromReplicatorObj(protobuf)
	}
	return DownstreamTopKRmvExtRemAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoMaxMinToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if max := protobuf.GetMaxminOp().GetMax(); max != nil {
		return MaxAddValue{}.FromReplicatorObj(protobuf)
	}
	return MinAddValue{}.FromReplicatorObj(protobuf)
}

func downstreamProtoTopSToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if protobuf.GetTopkinitOp() != nil {
		downOp = TopSInit(0).FromReplicatorObj(protobuf)
		return downOp
	}
	topSum := protobuf.GetTopsumOp()
	/*
		if len(topSum.GetElems()) == 1 {
			if topSum.GetIsPositive() {
				return DownstreamTopSAdd{}.FromReplicatorObj(protobuf)
			}
			return DownstreamTopSSub{}.FromReplicatorObj(protobuf)
		} else if topSum.GetIsPositive() {
			return DownstreamTopSAddAll{}.FromReplicatorObj(protobuf)
		}
		return DownstreamTopSSubAll{}.FromReplicatorObj(protobuf)*/
	elems := topSum.GetElems()
	if len(elems) == 1 {
		if elems[0].GetScore() >= 0 {
			return DownstreamTopSAdd{}.FromReplicatorObj(protobuf)
		}
		return DownstreamTopSSub{}.FromReplicatorObj(protobuf)
	}
	nPositive := int(topSum.GetPositiveLen())
	if nPositive == len(elems) {
		return DownstreamTopSAddAll{}.FromReplicatorObj(protobuf)
	}
	if nPositive == 0 {
		return DownstreamTopSSubAll{}.FromReplicatorObj(protobuf)
	}
	return DownstreamTopSAddAndSubAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoTopKToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if protobuf.GetTopkinitOp() != nil {
		downOp = TopKInit(0).FromReplicatorObj(protobuf)
		return downOp
	}
	adds := protobuf.GetTopkOp().GetAdds()
	if len(adds) == 1 {
		return DownstreamSimpleTopKAdd{}.FromReplicatorObj(protobuf)
	}
	return DownstreamSimpleTopKAddAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoFlagEWToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	flag := protobuf.GetFlagOp()
	if enable := flag.GetEnableEW(); enable != nil {
		return DownstreamEnableFlagEW{}.FromReplicatorObj(protobuf)
	}
	return DownstreamDisableFlagEW{}.FromReplicatorObj(protobuf)
}

func downstreamProtoFlagDWToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	flag := protobuf.GetFlagOp()
	if disable := flag.GetDisableDW(); disable != nil {
		return DownstreamDisableFlagDW{}.FromReplicatorObj(protobuf)
	}
	return DownstreamEnableFlagDW{}.FromReplicatorObj(protobuf)
}

func downstreamProtoFlagLWWToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	flag := protobuf.GetFlagOp()
	if enable := flag.GetEnableLWW(); enable != nil {
		return DownstreamEnableFlagLWW{}.FromReplicatorObj(protobuf)
	}
	return DownstreamDisableFlagLWW{}.FromReplicatorObj(protobuf)
}

func downstreamProtoBCounterToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	bcounter := protobuf.GetBcounterOp()
	if inc := bcounter.GetInc(); inc != nil {
		return DownstreamIncBCounter{}.FromReplicatorObj(protobuf)
	}
	if dec := bcounter.GetDec(); dec != nil {
		return DownstreamDecBCounter{}.FromReplicatorObj(protobuf)
	}
	if transfer := bcounter.GetTransfer(); transfer != nil {
		return TransferCounter{}.FromReplicatorObj(protobuf)
	}
	return SetCounterBound{}.FromReplicatorObj(protobuf)
}

func downstreamProtoPairCounterToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	pairCounter := protobuf.GetPairCounterOp()
	isInc := pairCounter.GetIsInc()
	firstChange, secondChange := pairCounter.GetFirstChange(), pairCounter.GetSecondChange()
	if isInc {
		if firstChange != 0 && secondChange != 0 {
			return IncrementBoth{}.FromReplicatorObj(protobuf)
		} else if firstChange != 0 {
			return IncrementFirst(0).FromReplicatorObj(protobuf)
		} else {
			return IncrementSecond(0).FromReplicatorObj(protobuf)
		}
	} //else:
	if firstChange != 0 && secondChange != 0 {
		return DecrementBoth{}.FromReplicatorObj(protobuf)
	} else if firstChange != 0 {
		return DecrementFirst(0).FromReplicatorObj(protobuf)
	} //else:
	return DecrementSecond(0).FromReplicatorObj(protobuf)
}

func downstreamProtoCounterArrayToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	arrayCounter := protobuf.GetArrayCounterOp()
	isInc := arrayCounter.GetIsInc()
	if isInc {
		switch arrayCounter.Upd.(type) {
		case *proto.ProtoArrayCounterDownstream_Inc:
			return CounterArrayIncrement{}.FromReplicatorObj(protobuf)
		case *proto.ProtoArrayCounterDownstream_IncAll:
			return CounterArrayIncrementAll(0).FromReplicatorObj(protobuf)
		case *proto.ProtoArrayCounterDownstream_IncMulti:
			return CounterArrayIncrementMulti(nil).FromReplicatorObj(protobuf)
		case *proto.ProtoArrayCounterDownstream_IncSub:
			return CounterArrayIncrementSub{}.FromReplicatorObj(protobuf)
		}
		/*if incProto := arrayCounter.GetInc(); incProto != nil {
			return CounterArrayIncrement{}.FromReplicatorObj(protobuf)
		} else if incAllProto := arrayCounter.GetIncAll(); incAllProto != nil {
			return CounterArrayIncrementAll(0).FromReplicatorObj(protobuf)
		} else if incMultProto := arrayCounter.GetIncMulti(); incMultProto != nil {
			return CounterArrayIncrementMulti([]int64{}).FromReplicatorObj(protobuf)
		} else if incSubProto := arrayCounter.GetIncSub(); incSubProto != nil {
			return CounterArrayIncrementSub{}.FromReplicatorObj(protobuf)
		}*/
	} else {
		switch arrayCounter.Upd.(type) {
		case *proto.ProtoArrayCounterDownstream_Inc:
			return CounterArrayDecrement{}.FromReplicatorObj(protobuf)
		case *proto.ProtoArrayCounterDownstream_IncAll:
			return CounterArrayDecrementAll(0).FromReplicatorObj(protobuf)
		case *proto.ProtoArrayCounterDownstream_IncMulti:
			return CounterArrayDecrementMulti(nil).FromReplicatorObj(protobuf)
		case *proto.ProtoArrayCounterDownstream_IncSub:
			return CounterArrayDecrementSub{}.FromReplicatorObj(protobuf)
		}
		/*if incProto := arrayCounter.GetInc(); incProto != nil {
			return CounterArrayDecrement{}.FromReplicatorObj(protobuf)
		} else if incAllProto := arrayCounter.GetIncAll(); incAllProto != nil {
			return CounterArrayDecrementAll(0).FromReplicatorObj(protobuf)
		} else if incMultProto := arrayCounter.GetIncMulti(); incMultProto != nil {
			return CounterArrayDecrementMulti([]int64{}).FromReplicatorObj(protobuf)
		} else if incSubProto := arrayCounter.GetIncSub(); incSubProto != nil {
			return CounterArrayDecrementSub{}.FromReplicatorObj(protobuf)
		}*/
	}
	return CounterArraySetSize(0).FromReplicatorObj(protobuf)
}

func downstreamProtoFloatArrayToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	arrayFloat := protobuf.GetArrayFloatOp()
	isInc := arrayFloat.GetIsInc()
	if isInc {
		switch arrayFloat.Upd.(type) {
		case *proto.ProtoArrayFloatDownstream_Inc:
			return FloatArrayIncrement{}.FromReplicatorObj(protobuf)
		case *proto.ProtoArrayFloatDownstream_IncAll:
			return FloatArrayIncrementAll(0).FromReplicatorObj(protobuf)
		case *proto.ProtoArrayFloatDownstream_IncMulti:
			return FloatArrayIncrementMulti(nil).FromReplicatorObj(protobuf)
		case *proto.ProtoArrayFloatDownstream_IncSub:
			return FloatArrayIncrementSub{}.FromReplicatorObj(protobuf)
		case *proto.ProtoArrayFloatDownstream_IncRange:
			return FloatArrayIncrementRange{}.FromReplicatorObj(protobuf)
		}
		/*if incProto := arrayFloat.GetInc(); incProto != nil {
			return FloatArrayIncrement{}.FromReplicatorObj(protobuf)
		} else if incAllProto := arrayFloat.GetIncAll(); incAllProto != nil {
			return FloatArrayIncrementAll(0).FromReplicatorObj(protobuf)
		} else if incMultProto := arrayFloat.GetIncMulti(); incMultProto != nil {
			return FloatArrayIncrementMulti([]float64{}).FromReplicatorObj(protobuf)
		} else if incSubProto := arrayFloat.GetIncSub(); incSubProto != nil {
			return FloatArrayIncrementSub{}.FromReplicatorObj(protobuf)
		}*/
	} else {
		switch arrayFloat.Upd.(type) {
		case *proto.ProtoArrayFloatDownstream_Inc:
			return FloatArrayDecrement{}.FromReplicatorObj(protobuf)
		case *proto.ProtoArrayFloatDownstream_IncAll:
			return FloatArrayDecrementAll(0).FromReplicatorObj(protobuf)
		case *proto.ProtoArrayFloatDownstream_IncMulti:
			return FloatArrayDecrementMulti(nil).FromReplicatorObj(protobuf)
		case *proto.ProtoArrayFloatDownstream_IncSub:
			return FloatArrayDecrementSub{}.FromReplicatorObj(protobuf)
		case *proto.ProtoArrayFloatDownstream_IncRange:
			return FloatArrayDecrementRange{}.FromReplicatorObj(protobuf)
		}
		/*if incProto := arrayFloat.GetInc(); incProto != nil {
			return FloatArrayDecrement{}.FromReplicatorObj(protobuf)
		} else if incAllProto := arrayFloat.GetIncAll(); incAllProto != nil {
			return FloatArrayDecrementAll(0).FromReplicatorObj(protobuf)
		} else if incMultProto := arrayFloat.GetIncMulti(); incMultProto != nil {
			return FloatArrayDecrementMulti([]float64{}).FromReplicatorObj(protobuf)
		} else if incSubProto := arrayFloat.GetIncSub(); incSubProto != nil {
			return FloatArrayDecrementSub{}.FromReplicatorObj(protobuf)
		}*/
	}
	return FloatArraySetSize(0).FromReplicatorObj(protobuf)
}

func downstreamProtoByteArrayToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	arrayBytes := protobuf.GetByteArrayOp()
	switch arrayBytes.Upd.(type) {
	case *proto.ProtoByteArrayDownstream_SetValue:
		return DownstreamByteArraySetValue{}.FromReplicatorObj(protobuf)
	case *proto.ProtoByteArrayDownstream_SetData:
		return DownstreamByteArraySetData{}.FromReplicatorObj(protobuf)
	case *proto.ProtoByteArrayDownstream_IntInc:
		return ByteArrayIncrement{}.FromReplicatorObj(protobuf)
	case *proto.ProtoByteArrayDownstream_FloatInc:
		return ByteArrayFloatInc{}.FromReplicatorObj(protobuf)
	case *proto.ProtoByteArrayDownstream_SetArrayInit:
		return ByteArraySetDataInitialize{}.FromReplicatorObj(protobuf)
	}
	return nil
}

/*downOp = downstreamProtoSimpleDateToAntidoteDownstream(protobuf)
downOp = downstreamProtoSetWDateToAntidoteDownstream(protobuf)
downOp = downstreamProtoIncWDateToAntidoteDownstream(protobuf)
downOp = downstreamProtoSetOnlyWDateToAntidoteDownstream(protobuf)*/

func downstreamProtoSimpleDateToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	dateProto := protobuf.GetSimpleDateOp()
	if isInitialSet := dateProto.GetIsInitialSet(); isInitialSet {
		return DownstreamSetInitialDate(0).FromReplicatorObj(protobuf)
	}
	return DownstreamIncMS(0).FromReplicatorObj(protobuf)
}

func downstreamProtoSetWDateToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	dateProto := protobuf.GetSetWDateOp()
	if inc := dateProto.GetSetWInc(); inc != nil {
		return DownstreamIncTsSetW{}.FromReplicatorObj(protobuf)
	}
	return DownstreamSetTsSetW{}.FromReplicatorObj(protobuf)
}

func downstreamProtoIncWDateToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	dateProto := protobuf.GetIncWDateOp()
	if inc := dateProto.GetIncWInc(); inc != nil {
		return DownstreamIncTsIncW{}.FromReplicatorObj(protobuf)
	}
	return DownstreamSetTsIncW{}.FromReplicatorObj(protobuf)
}

func downstreamProtoMultiArrayToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	multiArray := protobuf.GetMultiArrayOp()
	arrayType := multiArray.GetType()
	//fmt.Printf("[CRDTProtoLib]Multi array proto downstream to antidote. Start. ArrayType: %+v\n", arrayType)
	switch arrayType {
	case proto.MultiArrayType_MA_INT:
		counterUpd := multiArray.GetIntUpd() //Inc, Multi, Sub
		if inc := counterUpd.GetIncSingle(); inc != nil {
			return MultiArrayIncIntSingle{}.FromReplicatorObj(protobuf)
		}
		if incMult := counterUpd.GetInc(); incMult != nil {
			return MultiArrayIncInt(nil).FromReplicatorObj(protobuf)
		}
		if incSub := counterUpd.GetIncPos(); incSub != nil {
			return MultiArrayIncIntPositions{}.FromReplicatorObj(protobuf)
		}
		if incRange := counterUpd.GetIncRange(); incRange != nil {
			return MultiArrayIncIntRange{}.FromReplicatorObj(protobuf)
		}
	case proto.MultiArrayType_MA_FLOAT:
		floatUpd := multiArray.GetFloatUpd()
		if inc := floatUpd.GetIncSingle(); inc != nil {
			return MultiArrayIncFloatSingle{}.FromReplicatorObj(protobuf)
		}
		if incMult := floatUpd.GetInc(); incMult != nil {
			return MultiArrayIncFloat(nil).FromReplicatorObj(protobuf)
		}
		if incSub := floatUpd.GetIncPos(); incSub != nil {
			return MultiArrayIncFloatPositions{}.FromReplicatorObj(protobuf)
		}
		if incRange := floatUpd.GetIncRange(); incRange != nil {
			return MultiArrayIncFloatRange{}.FromReplicatorObj(protobuf)
		}
	case proto.MultiArrayType_MA_DATA:
		dataUpd := multiArray.GetDataUpd()
		if inc := dataUpd.GetSetSingle(); inc != nil {
			return DownstreamMultiArraySetRegisterSingle{}.FromReplicatorObj(protobuf)
		}
		if incMult := dataUpd.GetSet(); incMult != nil {
			return DownstreamMultiArraySetRegister{}.FromReplicatorObj(protobuf)
		}
		if incSub := dataUpd.GetSetPos(); incSub != nil {
			return DownstreamMultiArraySetRegisterPositions{}.FromReplicatorObj(protobuf)
		}
		if incRange := dataUpd.GetSetRange(); incRange != nil {
			return DownstreamMultiArraySetRegisterRange{}.FromReplicatorObj(protobuf)
		}
	case proto.MultiArrayType_MA_AVG:
		avgUpd := multiArray.GetAvgUpd()
		if inc := avgUpd.GetIncSingle(); inc != nil {
			return MultiArrayIncAvgSingle{}.FromReplicatorObj(protobuf)
		}
		if incMult := avgUpd.GetInc(); incMult != nil {
			return MultiArrayIncAvg{}.FromReplicatorObj(protobuf)
		}
		if incSub := avgUpd.GetIncPos(); incSub != nil {
			return MultiArrayIncAvgPositions{}.FromReplicatorObj(protobuf)
		}
		if incRange := avgUpd.GetIncRange(); incRange != nil {
			return MultiArrayIncAvgRange{}.FromReplicatorObj(protobuf)
		}
	case proto.MultiArrayType_MA_MULTI:
		return DownstreamMultiArrayUpdateAll{}.FromReplicatorObj(protobuf)
	case proto.MultiArrayType_MA_SIZE:
		return MultiArraySetSizes{}.FromReplicatorObj(protobuf)
	default:
		fmt.Printf("[CRDTProtoLib][ERROR]Unknown type of multi array downstream. ArrayType: %+v.\n", arrayType)
	}
	//fmt.Printf("[CRDTProtoLib][ERROR]Did not match downstream. ArrayType: %+v.\n. MultiArrayProto: %+v\n", arrayType, protobuf)
	return nil
}

func downstreamProtoCompactArrayToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	arrayProto := protobuf.GetCompactArrayOp()
	if setValue := arrayProto.GetSetValue(); setValue != nil {
		return DownstreamCompactArraySetValue{}.FromReplicatorObj(protobuf)
	} else if intInc := arrayProto.GetIntInc(); intInc != nil {
		if intInc.GetInc() >= 0 {
			return CompactArrayIncrement{}.FromReplicatorObj(protobuf)
		}
		return CompactArrayDecrement{}.FromReplicatorObj(protobuf)
	} else if floatInc := arrayProto.GetFloatInc(); floatInc != nil {
		if floatInc.GetInc() >= 0 {
			return CompactArrayFloatInc{}.FromReplicatorObj(protobuf)
		}
		return CompactArrayFloatDec{}.FromReplicatorObj(protobuf)
	} else if setArray := arrayProto.GetSetArray(); setArray != nil {
		return DownstreamCompactArraySetArray{}.FromReplicatorObj(protobuf)
	} //Size
	return CompactArraySetSize(0).FromReplicatorObj(protobuf)
}

func downstreamProtoStringArrayToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	arrayProto := protobuf.GetStringArrayOp()
	if setValue := arrayProto.GetSetValue(); setValue != nil {
		return DownstreamStringArraySetValue{}.FromReplicatorObj(protobuf)
	} else if setArray := arrayProto.GetSetArray(); setArray != nil {
		return DownstreamStringArraySetArray{}.FromReplicatorObj(protobuf)
	} else if setArrayInit := arrayProto.GetSetArrayInit(); setArrayInit != nil {
		return StringArraySetArrayInitialize{}.FromReplicatorObj(protobuf)
	} //Size
	return StringArraySetSize(0).FromReplicatorObj(protobuf)
}

func downstreamProtoMapCounterToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	mapOp := protobuf.GetMapCounterOp()
	dataType, isDec := mapOp.GetDataType(), mapOp.GetIsDec()
	//fmt.Printf("[CRDTProtoLib]Map counter proto downstream to antidote. Start. Inner proto type: %T, Data type: %v, isDec: %v.\n", mapOp.Upd, dataType, isDec)
	switch mapOp.Upd.(type) {
	case *proto.ProtoMapCounterDownstream_IntInc, *proto.ProtoMapCounterDownstream_DoubleInc:
		return protoMapCounterIncToReplicatorObj(protobuf, isDec, dataType)
	case *proto.ProtoMapCounterDownstream_IntIncAll, *proto.ProtoMapCounterDownstream_DoubleIncAll:
		return protoMapCounterIncAllToReplicatorObj(protobuf, isDec, dataType)
	case *proto.ProtoMapCounterDownstream_IntIncMult, *proto.ProtoMapCounterDownstream_DoubleIncMult:
		return protoMapCounterIncMultToReplicatorObj(protobuf, isDec, dataType)
	case *proto.ProtoMapCounterDownstream_Init:
		return protoMapCounterInitToReplicatorObj(protobuf, dataType)
	}
	return nil
	/*if mapOp.GetIntInc() != nil || mapOp.GetDoubleInc() != nil {
		return protoMapCounterIncToReplicatorObj(protobuf, isDec, dataType)
	} else if mapOp.GetIntIncAll() != nil || mapOp.GetDoubleIncAll() != nil {
		return protoMapCounterIncAllToReplicatorObj(protobuf, isDec, dataType)
	} else if mapOp.GetIntIncMult() != nil || mapOp.GetDoubleIncMult() != nil {
		return protoMapCounterIncMultToReplicatorObj(protobuf, isDec, dataType)
	} else {
		return protoMapCounterInitToReplicatorObj(protobuf, dataType)
	}*/
}

func protoMapCounterIncToReplicatorObj(protobuf *proto.ProtoOpDownstream, isDec bool, dataType proto.DATAType) DownstreamArguments {
	if !isDec {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapInc[int]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT64:
			return CounterMapInc[int64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT32:
			return CounterMapInc[int32]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT16:
			return CounterMapInc[int16]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT8:
			return CounterMapInc[int8]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapInc[float64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapInc[float32]{}.FromReplicatorObj(protobuf)
		}
	} else {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapDec[int]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT64:
			return CounterMapDec[int64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT32:
			return CounterMapDec[int32]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT16:
			return CounterMapDec[int16]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT8:
			return CounterMapDec[int8]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapDec[float64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapDec[float32]{}.FromReplicatorObj(protobuf)
		}
	}
	return nil
}

func protoMapCounterIncAllToReplicatorObj(protobuf *proto.ProtoOpDownstream, isDec bool, dataType proto.DATAType) DownstreamArguments {
	if !isDec {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapIncAll[int]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT64:
			return CounterMapIncAll[int64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT32:
			return CounterMapIncAll[int32]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT16:
			return CounterMapIncAll[int16]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT8:
			return CounterMapIncAll[int8]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapIncAll[float64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapIncAll[float32]{}.FromReplicatorObj(protobuf)
		}
	} else {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapDecAll[int]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT64:
			return CounterMapDecAll[int64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT32:
			return CounterMapDecAll[int32]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT16:
			return CounterMapDecAll[int16]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT8:
			return CounterMapDecAll[int8]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapDecAll[float64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapDecAll[float32]{}.FromReplicatorObj(protobuf)
		}
	}
	return nil
}

func protoMapCounterIncMultToReplicatorObj(protobuf *proto.ProtoOpDownstream, isDec bool, dataType proto.DATAType) DownstreamArguments {
	//fmt.Printf("[CRDTProtoLib]Map counter proto is IncMult. Inner proto type: %T, Data type: %v, isDec: %v.\n", protobuf.GetMapCounterOp().Upd, dataType, isDec)
	if !isDec {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapIncMult[int]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT64:
			return CounterMapIncMult[int64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT32:
			//fmt.Printf("[CRDTProtoLib]Map counter proto is IncMult[int32]. Matched successfully.\n")
			return CounterMapIncMult[int32]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT16:
			return CounterMapIncMult[int16]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT8:
			return CounterMapIncMult[int8]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapIncMult[float64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapIncMult[float32]{}.FromReplicatorObj(protobuf)
		}
	} else {
		switch dataType {
		case proto.DATAType_INT:
			return CounterMapDecMult[int]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT64:
			return CounterMapDecMult[int64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT32:
			return CounterMapDecMult[int32]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT16:
			return CounterMapDecMult[int16]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_INT8:
			return CounterMapDecMult[int8]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT64:
			return CounterMapDecMult[float64]{}.FromReplicatorObj(protobuf)
		case proto.DATAType_FLOAT32:
			return CounterMapDecMult[float32]{}.FromReplicatorObj(protobuf)
		}
	}
	return nil
}

func protoMapCounterInitToReplicatorObj(protobuf *proto.ProtoOpDownstream, dataType proto.DATAType) DownstreamArguments {
	switch dataType {
	case proto.DATAType_INT:
		return CounterMapInit[int](0).FromReplicatorObj(protobuf)
	case proto.DATAType_INT64:
		return CounterMapInit[int64](0).FromReplicatorObj(protobuf)
	case proto.DATAType_INT32:
		return CounterMapInit[int32](0).FromReplicatorObj(protobuf)
	case proto.DATAType_INT16:
		return CounterMapInit[int16](0).FromReplicatorObj(protobuf)
	case proto.DATAType_INT8:
		return CounterMapInit[int8](0).FromReplicatorObj(protobuf)
	case proto.DATAType_FLOAT64:
		return CounterMapInit[float64](0).FromReplicatorObj(protobuf)
	case proto.DATAType_FLOAT32:
		return CounterMapInit[float32](0).FromReplicatorObj(protobuf)
	}
	return nil
}

func mapCounterToProtoState(protobuf *proto.ProtoState, ts *clocksi.Timestamp, replicaID uint16) (crdt CRDT) {
	mapState := protobuf.GetMapCounter()
	dataType := mapState.GetDataType()
	switch dataType {
	case proto.DATAType_INT:
		return (&CounterMapCrdt[int]{}).FromProtoState(protobuf, ts, replicaID)
	case proto.DATAType_INT64:
		return (&CounterMapCrdt[int64]{}).FromProtoState(protobuf, ts, replicaID)
	case proto.DATAType_INT32:
		return (&CounterMapCrdt[int32]{}).FromProtoState(protobuf, ts, replicaID)
	case proto.DATAType_INT16:
		return (&CounterMapCrdt[int16]{}).FromProtoState(protobuf, ts, replicaID)
	case proto.DATAType_INT8:
		return (&CounterMapCrdt[int8]{}).FromProtoState(protobuf, ts, replicaID)
	case proto.DATAType_FLOAT64:
		return (&CounterMapCrdt[float64]{}).FromProtoState(protobuf, ts, replicaID)
	case proto.DATAType_FLOAT32:
		return (&CounterMapCrdt[float32]{}).FromProtoState(protobuf, ts, replicaID)
	}
	return
}

/***OTHER HELPERS***/

func mapEntriesToProto(entries map[string]Element) (converted []*proto.ApbMapNestedUpdate) {
	converted = make([]*proto.ApbMapNestedUpdate, len(entries))
	crdtType := proto.CRDTType_LWWREG
	i := 0
	for key, elem := range entries {
		converted[i] = &proto.ApbMapNestedUpdate{
			Key:    &proto.ApbMapKey{Key: unsafe.Slice(unsafe.StringData(key), len(key)), Type: &crdtType},
			Update: &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Regop{Regop: &proto.ApbRegUpdate{Value: []byte(elem)}}},
		}
		i++
	}
	return
}

func entriesToApbMapEntries(entries map[string]Element) (protos []*proto.ApbMapEntry) {
	protos = make([]*proto.ApbMapEntry, len(entries))
	crdtType := proto.CRDTType_LWWREG
	i := 0
	for key, elem := range entries {
		protos[i] = &proto.ApbMapEntry{
			Key:   &proto.ApbMapKey{Key: unsafe.Slice(unsafe.StringData(key), len(key)), Type: &crdtType},
			Value: &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Reg{Reg: &proto.ApbGetRegResp{Value: []byte(elem)}}}}
		i++
	}
	return
}

func crdtsToApbMapEntries(states map[string]State) (protos []*proto.ApbMapEntry) {
	protos = make([]*proto.ApbMapEntry, len(states))
	i := 0
	for key, state := range states {
		crdtType := state.GetCRDTType()
		protos[i] = &proto.ApbMapEntry{
			Key:   &proto.ApbMapKey{Key: unsafe.Slice(unsafe.StringData(key), len(key)), Type: &crdtType},
			Value: state.(ProtoState).ToReadResp(),
		}
		i++
	}
	return
}

func stringArrayToMapKeyArray(keys []string) (converted []*proto.ApbMapKey) {
	converted = make([]*proto.ApbMapKey, len(keys))
	crdtType := proto.CRDTType_LWWREG
	for i, key := range keys {
		converted[i] = &proto.ApbMapKey{Key: unsafe.Slice(unsafe.StringData(key), len(key)), Type: &crdtType}
	}
	return
}

func byteArrayToStringArray(keysBytes [][]byte) (keys []string) {
	keys = make([]string, len(keysBytes))
	//for i := 0; i < len(keysBytes); i++ {
	//keys[i] = string(keysBytes[i])
	for i, data := range keysBytes {
		keys[i] = unsafe.String(&(data[0]), len(data))
	}
	return
}

func stringArrayToByteArray(keys []string) (keysBytes [][]byte) {
	keysBytes = make([][]byte, len(keys))
	for i, key := range keys {
		//keysBytes[i] = []byte(key)
		keysBytes[i] = unsafe.Slice(unsafe.StringData(key), len(key))
	}
	return
}

func createSliceNestedOps(upds []EmbMapUpdate) (converted []*proto.ApbMapNestedUpdate) {
	converted = make([]*proto.ApbMapNestedUpdate, len(upds))
	for i, upd := range upds {
		crdtType, byteKeys, protoUpd := upd.Upd.GetCRDTType(), unsafe.Slice(unsafe.StringData(upd.Key), len(upd.Key)), upd.Upd.(ProtoUpd).ToUpdateObject()
		/*if crdtType != proto.CRDTType_LWWREG {
			fmt.Printf("[CRDTProtoLib]Creating map nested op. Key: %s, CRDT type: %v, Upd type: %T. Proto update: %+v\n", upd.Key, crdtType, upd.Upd, protoUpd)
		}*/
		converted[i] = &proto.ApbMapNestedUpdate{Key: &proto.ApbMapKey{Key: byteKeys, Type: &crdtType}, Update: protoUpd}
	}
	return
}

func createMapNestedOps(upds map[string]UpdateArguments) (converted []*proto.ApbMapNestedUpdate) {
	converted = make([]*proto.ApbMapNestedUpdate, len(upds))
	i := 0
	for key, upd := range upds {
		crdtType, byteKeys, protoUpd := upd.GetCRDTType(), unsafe.Slice(unsafe.StringData(key), len(key)), upd.(ProtoUpd).ToUpdateObject()
		converted[i] = &proto.ApbMapNestedUpdate{Key: &proto.ApbMapKey{Key: byteKeys, Type: &crdtType}, Update: protoUpd}
		i++
	}
	return
}

func createMapGetValuesRead(readArgs ReadArguments) (protobuf *proto.ApbMapEmbPartialArgs) {
	crdtType, readType := readArgs.GetCRDTType(), readArgs.GetREADType()
	if _, ok := readArgs.(StateReadArguments); ok {
		return &proto.ApbMapEmbPartialArgs{Readtype: &readType}
	}
	return &proto.ApbMapEmbPartialArgs{Type: &crdtType, Readtype: &readType, Args: readArgs.(ProtoRead).ToPartialRead()}
}

func createProtoMapRemoves(rems map[string]map[Element]UniqueSet) (protos []*proto.ProtoORMapRemove) {
	protos = make([]*proto.ProtoORMapRemove, len(rems))
	i, j := 0, 0
	for key, elems := range rems {
		protoElems := make([]*proto.ProtoValueUniques, len(elems))
		for elem, uniques := range elems {
			protoElems[j] = &proto.ProtoValueUniques{Value: []byte(elem), Uniques: UniqueSetToUInt64Array(uniques)}
			j++
		}
		j = 0
		protos[i] = &proto.ProtoORMapRemove{Key: []byte(key), Elems: protoElems}
		i++
	}
	return
}

func createORMapDownRems(protos []*proto.ProtoORMapRemove) (rems map[string]map[Element]UniqueSet) {
	rems = make(map[string]map[Element]UniqueSet, len(protos))
	for _, remProto := range protos {
		elemsProto := remProto.GetElems()
		innerElems := make(map[Element]UniqueSet, len(elemsProto))
		for _, elemProto := range elemsProto {
			innerElems[Element(elemProto.GetValue())] = UInt64ArrayToUniqueSet(elemProto.GetUniques())
		}
		rems[string(remProto.GetKey())] = innerElems
	}
	return
}

/*****GENERIC*****/
func (args StateReadArguments) toPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{}
}
func (args StateReadArguments) fromPartialRead() (readArgs ReadArguments) {
	return args
}

/*****MISCELANEOUS*****/

func CreateMapUpdateFromProto(isAdd bool, adds map[string]*proto.ApbUpdateOp, rems map[string]struct{}) (protoBuf *proto.ApbMapUpdate) {
	protoBuf = &proto.ApbMapUpdate{}
	i := 0
	if isAdd {
		protoBuf.Updates = make([]*proto.ApbMapNestedUpdate, len(adds))
		for key, op := range adds {
			crdtType := op.GetBoundobject().GetType()
			protoBuf.Updates[i] = &proto.ApbMapNestedUpdate{
				Key:    &proto.ApbMapKey{Key: unsafe.Slice(unsafe.StringData(key), len(key)), Type: &crdtType},
				Update: op.GetOperation(),
			}
			i++
		}
	} else {
		crdtType := proto.CRDTType_LWWREG
		protoBuf.RemovedKeys = make([]*proto.ApbMapKey, len(rems))
		for key := range rems {
			//For now it's irrelevant the Type field
			protoBuf.RemovedKeys[i] = &proto.ApbMapKey{Key: unsafe.Slice(unsafe.StringData(key), len(key)), Type: &crdtType}
			i++
		}
	}
	return
}
