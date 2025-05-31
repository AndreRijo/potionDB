package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
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

	FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT)
}

/*****GLOBAL FUNCS*****/
func UpdateProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation, crdtType proto.CRDTType) (op UpdateArguments) {
	//fmt.Println("[CRDTProtoLib]Proto->Antidote. CRDTType: ", crdtType)
	if protobuf.GetResetop() != nil {
		return ResetOp{}
	}

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
	case proto.CRDTType_TOPK_RMV:
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
	case proto.CRDTType_MULTI_ARRAY:
		return updateMultiArrayProtoToAntidoteUpdate(protobuf)
	}

	return nil
}

func PartialReadOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs, crdtType proto.CRDTType, readType proto.READType) (read *ReadArguments) {
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

	//MultiArray
	case proto.READType_MULTI_DATA_INT:
		tmpRead = MultiArrayDataIntArguments(0).FromPartialRead(protobuf)
	case proto.READType_MULTI_DATA_COND:
		tmpRead = partialMultiDataCondOpToAntidoteRead(protobuf)
	case proto.READType_MULTI_COND:
		tmpRead = MultiArrayComparableArguments{}.FromPartialRead(protobuf)
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

	//Process
	case proto.READType_PROCESS:
		tmpRead = ReadProcessingObjectParams{}.FromPartialRead(protobuf)
	}

	return &tmpRead
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
	case proto.CRDTType_TOPK_RMV, proto.CRDTType_TOPSUM, proto.CRDTType_TOPK:
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
	case proto.CRDTType_MULTI_ARRAY:
		state = MultiArrayState{}.FromReadResp(protobuf)
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
	}

	return
}

func DownstreamProtoToAntidoteDownstream(protobuf *proto.ProtoOpDownstream, crdtType proto.CRDTType) (downOp DownstreamArguments) {
	if protobuf.GetTopkinitOp() != nil {
		downOp = TopKInit{}.FromReplicatorObj(protobuf)
		return downOp
	}
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
	case proto.CRDTType_MULTI_ARRAY:
		downOp = downstreamProtoMultiArrayToAntidoteDownstream(protobuf)
	}

	return
}

func StateProtoToCrdt(protobuf *proto.ProtoState, crdtType proto.CRDTType, ts *clocksi.Timestamp, replicaID int16) (crdt CRDT) {
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
	case proto.CRDTType_MULTI_ARRAY:
		crdt = (&MultiArrayCrdt{}).FromProtoState(protobuf, ts, replicaID)
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
	if len(protobuf.GetMapop().GetUpdates()) > 0 {
		if !protobuf.GetMapop().GetIsAddsArray() {
			return EmbMapUpdateAll{}.FromUpdateObject(protobuf)
		}
		return EmbMapUpdateAllArray{}.FromUpdateObject(protobuf)
	}
	return MapRemoveAll{}.FromUpdateObject(protobuf)
}

func updateTopkRmvProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if topKInit := protobuf.GetTopkinitop(); topKInit != nil {
		if topKInit.GetIsTopSum() {
			return TopSInit(0).FromUpdateObject(protobuf)
		}
		return TopKInit{}.FromUpdateObject(protobuf)
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
	adds := protobuf.GetTopkrmvop().GetAdds()
	if len(adds) == 1 {
		if adds[0].GetScore() >= 0 {
			return TopSAdd{}.FromUpdateObject(protobuf)
		} else {
			return TopSSub{}.FromUpdateObject(protobuf)
		}
	} else if len(adds) >= 0 {
		return TopSAddAll{}.FromUpdateObject(protobuf)
	}
	return TopSSubAll{}.FromUpdateObject(protobuf)
}

func updateTopkProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if protobuf.GetTopkinitop() != nil {
		return TopKInit{}.FromUpdateObject(protobuf)
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
	if inc := arrayCounter.GetInc(); inc != nil {
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
	return CounterArraySetSize(0).FromUpdateObject(protobuf)
}

func updateMultiArrayProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	multiArray := protobuf.GetMultiarrayop()
	arrayType := multiArray.GetType()
	//fmt.Printf("[CRDTProtoLib]Multi array update proto to antidote. Start. ArrayType: %+v.\n", arrayType)
	switch arrayType {
	case proto.MultiArrayType_INT:
		counterUpd := multiArray.GetIntUpd() //Inc, Multi, Sub
		if inc := counterUpd.GetIncSingle(); inc != nil {
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
		}
	case proto.MultiArrayType_FLOAT:
		floatUpd := multiArray.GetFloatUpd()
		if inc := floatUpd.GetIncSingle(); inc != nil {
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
		}
	case proto.MultiArrayType_DATA:
		dataUpd := multiArray.GetDataUpd()
		if inc := dataUpd.GetSetSingle(); inc != nil {
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
		}
	case proto.MultiArrayType_AVG_TYPE:
		avgUpd := multiArray.GetAvgUpd()
		if inc := avgUpd.GetIncSingle(); inc != nil {
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
		}
	case proto.MultiArrayType_MULTI:
		return MultiArrayUpdateAll{}.FromUpdateObject(protobuf)
	case proto.MultiArrayType_SIZE:
		return MultiArraySetSizes{}.FromUpdateObject(protobuf)
	default:
		fmt.Printf("[CRDTProtoLib][ERROR]Unknown type of multi array update. ArrayType: %+v.\n", arrayType)
	}
	//fmt.Printf("[CRDTProtoLib][ERROR]Did not match update. ArrayType: %+v.\n. MultiArrayProto: %+v\n", arrayType, protobuf)
	return nil
}

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

func partialMultiDataCondRespToAntidoteState(protobuf *proto.ApbReadObjectResp) (state State) {
	arrayType := protobuf.GetPartread().GetMultiarray().GetType()
	switch arrayType {
	case proto.MultiArrayType_INT:
		return MultiArrayDataSliceIntPosState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_FLOAT:
		return MultiArrayDataSliceFloatPosState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_AVG_TYPE:
		return MultiArrayDataSliceAvgPosState{}.FromReadResp(protobuf)
	}
	return nil
}

func partialMultiFullRespToAntidoteRead(protobuf *proto.ApbReadObjectResp) (state State) {
	arrayType := protobuf.GetPartread().GetMultiarray().GetType()
	switch arrayType {
	case proto.MultiArrayType_INT:
		return IntArrayState([]int64{}).FromReadResp(protobuf)
	case proto.MultiArrayType_FLOAT:
		return FloatArrayState([]float64{}).FromReadResp(protobuf)
	case proto.MultiArrayType_AVG_TYPE:
		return AvgArrayState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_DATA:
		return DataArrayState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_MULTI:
		return MultiArrayState{}.FromReadResp(protobuf)
	}
	return nil
}
func partialMultiSingleRespToAntidoteRead(protobuf *proto.ApbReadObjectResp) (state State) {
	arrayType := protobuf.GetPartread().GetMultiarray().GetType()
	switch arrayType {
	case proto.MultiArrayType_INT:
		return IntArraySingleState(0).FromReadResp(protobuf)
	case proto.MultiArrayType_FLOAT:
		return FloatArraySingleState(0).FromReadResp(protobuf)
	case proto.MultiArrayType_AVG_TYPE:
		return AvgArraySingleState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_DATA:
		return DataArraySingleState{}.FromReadResp(protobuf)
	case proto.MultiArrayType_MULTI:
		return MultiArraySingleState{}.FromReadResp(protobuf)
	}
	return nil
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
	if protobuf.GetMap().Getvalues.Args == nil {
		return GetValuesArguments{}.FromPartialRead(protobuf)
	}
	return EmbMapPartialArguments{}.FromPartialRead(protobuf)
}

func partialMultiDataCondOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs) (read ReadArguments) {
	readType := protobuf.GetMultiarray().GetTypes()[0]
	switch readType {
	case proto.MultiArrayType_INT:
		return MultiArrayDataIntComparableArguments{}.FromPartialRead(protobuf)
	case proto.MultiArrayType_FLOAT:
		return MultiArrayDataFloatComparableArguments{}.FromPartialRead(protobuf)
	case proto.MultiArrayType_AVG_TYPE:
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
	if adds := protobuf.GetRwembmapOp().GetAdds(); adds != nil {
		if !adds.GetIsArray() {
			return DownstreamRWEmbMapUpdateAll{}.FromReplicatorObj(protobuf)
		}
		return DownstreamRWEmbMapUpdateAllArray{}.FromReplicatorObj(protobuf)
	}
	return DownstreamRWEmbMapRemoveAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoTopKRmvToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
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

func downstreamProtoMaxMinToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if max := protobuf.GetMaxminOp().GetMax(); max != nil {
		return MaxAddValue{}.FromReplicatorObj(protobuf)
	}
	return MinAddValue{}.FromReplicatorObj(protobuf)
}

func downstreamProtoTopSToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	topSum := protobuf.GetTopsumOp()
	if len(topSum.GetElems()) == 1 {
		if topSum.GetIsPositive() {
			return DownstreamTopSAdd{}.FromReplicatorObj(protobuf)
		}
		return DownstreamTopSSub{}.FromReplicatorObj(protobuf)
	} else if topSum.GetIsPositive() {
		return DownstreamTopSAddAll{}.FromReplicatorObj(protobuf)
	}
	return DownstreamTopSSubAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoTopKToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
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
		if incProto := arrayCounter.GetInc(); incProto != nil {
			return CounterArrayIncrement{}.FromReplicatorObj(protobuf)
		} else if incAllProto := arrayCounter.GetIncAll(); incAllProto != nil {
			return CounterArrayIncrementAll(0).FromReplicatorObj(protobuf)
		} else if incMultProto := arrayCounter.GetIncMulti(); incMultProto != nil {
			return CounterArrayIncrementMulti([]int64{}).FromReplicatorObj(protobuf)
		} else if incSubProto := arrayCounter.GetIncSub(); incSubProto != nil {
			return CounterArrayIncrementSub{}.FromReplicatorObj(protobuf)
		}
	} else {
		if incProto := arrayCounter.GetInc(); incProto != nil {
			return CounterArrayDecrement{}.FromReplicatorObj(protobuf)
		} else if incAllProto := arrayCounter.GetIncAll(); incAllProto != nil {
			return CounterArrayDecrementAll(0).FromReplicatorObj(protobuf)
		} else if incMultProto := arrayCounter.GetIncMulti(); incMultProto != nil {
			return CounterArrayDecrementMulti([]int64{}).FromReplicatorObj(protobuf)
		} else if incSubProto := arrayCounter.GetIncSub(); incSubProto != nil {
			return CounterArrayDecrementSub{}.FromReplicatorObj(protobuf)
		}
	}
	return CounterArraySetSize(0).FromReplicatorObj(protobuf)
}

func downstreamProtoMultiArrayToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	multiArray := protobuf.GetMultiArrayOp()
	arrayType := multiArray.GetType()
	//fmt.Printf("[CRDTProtoLib]Multi array proto downstream to antidote. Start. ArrayType: %+v\n", arrayType)
	switch arrayType {
	case proto.MultiArrayType_INT:
		counterUpd := multiArray.GetIntUpd() //Inc, Multi, Sub
		if inc := counterUpd.GetIncSingle(); inc != nil {
			return MultiArrayIncIntSingle{}.FromReplicatorObj(protobuf)
		}
		if incMult := counterUpd.GetInc(); incMult != nil {
			return MultiArrayIncInt([]int64{}).FromReplicatorObj(protobuf)
		}
		if incSub := counterUpd.GetIncPos(); incSub != nil {
			return MultiArrayIncIntPositions{}.FromReplicatorObj(protobuf)
		}
		if incRange := counterUpd.GetIncRange(); incRange != nil {
			return MultiArrayIncIntRange{}.FromReplicatorObj(protobuf)
		}
	case proto.MultiArrayType_FLOAT:
		floatUpd := multiArray.GetFloatUpd()
		if inc := floatUpd.GetIncSingle(); inc != nil {
			return MultiArrayIncFloatSingle{}.FromReplicatorObj(protobuf)
		}
		if incMult := floatUpd.GetInc(); incMult != nil {
			return MultiArrayIncFloat([]float64{}).FromReplicatorObj(protobuf)
		}
		if incSub := floatUpd.GetIncPos(); incSub != nil {
			return MultiArrayIncFloatPositions{}.FromReplicatorObj(protobuf)
		}
		if incRange := floatUpd.GetIncRange(); incRange != nil {
			return MultiArrayIncFloatRange{}.FromReplicatorObj(protobuf)
		}
	case proto.MultiArrayType_DATA:
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
	case proto.MultiArrayType_AVG_TYPE:
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
	case proto.MultiArrayType_MULTI:
		return DownstreamMultiArrayUpdateAll{}.FromReplicatorObj(protobuf)
	case proto.MultiArrayType_SIZE:
		return MultiArraySetSizes{}.FromReplicatorObj(protobuf)
	default:
		fmt.Printf("[CRDTProtoLib][ERROR]Unknown type of multi array downstream. ArrayType: %+v.\n", arrayType)
	}
	//fmt.Printf("[CRDTProtoLib][ERROR]Did not match downstream. ArrayType: %+v.\n. MultiArrayProto: %+v\n", arrayType, protobuf)
	return nil
}

/***OTHER HELPERS***/

func mapEntriesToProto(entries map[string]Element) (converted []*proto.ApbMapNestedUpdate) {
	converted = make([]*proto.ApbMapNestedUpdate, len(entries))
	crdtType := proto.CRDTType_LWWREG
	i := 0
	for key, elem := range entries {
		converted[i] = &proto.ApbMapNestedUpdate{
			Key:    &proto.ApbMapKey{Key: []byte(key), Type: &crdtType},
			Update: &proto.ApbUpdateOperation{Regop: &proto.ApbRegUpdate{Value: []byte(elem)}},
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
			Key:   &proto.ApbMapKey{Key: []byte(key), Type: &crdtType},
			Value: &proto.ApbReadObjectResp{Reg: &proto.ApbGetRegResp{Value: []byte(elem)}},
		}
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
			Key:   &proto.ApbMapKey{Key: []byte(key), Type: &crdtType},
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
		converted[i] = &proto.ApbMapKey{Key: []byte(key), Type: &crdtType}
	}
	return
}

func byteArrayToStringArray(keysBytes [][]byte) (keys []string) {
	keys = make([]string, len(keysBytes))
	for i := 0; i < len(keysBytes); i++ {
		keys[i] = string(keysBytes[i])
	}
	return
}

func stringArrayToByteArray(keys []string) (keysBytes [][]byte) {
	keysBytes = make([][]byte, len(keys))
	for i, key := range keys {
		keysBytes[i] = []byte(key)
	}
	return
}

func createSliceNestedOps(upds []EmbMapUpdate) (converted []*proto.ApbMapNestedUpdate) {
	converted = make([]*proto.ApbMapNestedUpdate, len(upds))
	for i, upd := range upds {
		crdtType, byteKeys, protoUpd := upd.GetCRDTType(), []byte(upd.Key), upd.Upd.(ProtoUpd).ToUpdateObject()
		converted[i] = &proto.ApbMapNestedUpdate{Key: &proto.ApbMapKey{Key: byteKeys, Type: &crdtType}, Update: protoUpd}
	}
	return
}

func createMapNestedOps(upds map[string]UpdateArguments) (converted []*proto.ApbMapNestedUpdate) {
	converted = make([]*proto.ApbMapNestedUpdate, len(upds))
	i := 0
	for key, upd := range upds {
		crdtType, byteKeys, protoUpd := upd.GetCRDTType(), []byte(key), upd.(ProtoUpd).ToUpdateObject()
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
	rems = make(map[string]map[Element]UniqueSet)
	for _, remProto := range protos {
		innerElems := make(map[Element]UniqueSet)
		for _, elemProto := range remProto.GetElems() {
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
				Key:    &proto.ApbMapKey{Key: []byte(key), Type: &crdtType},
				Update: op.GetOperation(),
			}
			i++
		}
	} else {
		crdtType := proto.CRDTType_LWWREG
		protoBuf.RemovedKeys = make([]*proto.ApbMapKey, len(rems))
		for key := range rems {
			//For now it's irrelevant the Type field
			protoBuf.RemovedKeys[i] = &proto.ApbMapKey{Key: []byte(key), Type: &crdtType}
			i++
		}
	}
	return
}
