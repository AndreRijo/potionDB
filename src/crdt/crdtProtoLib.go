package crdt

import (
	"potionDB/src/clocksi"
	"potionDB/src/proto"
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

//*****INTERFACES*****/
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
func UpdateProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation, crdtType proto.CRDTType) (op *UpdateArguments) {
	var tmpUpd UpdateArguments = NoOp{}

	if protobuf.GetResetop() != nil {
		tmpUpd = ResetOp{}
		return &tmpUpd
	}

	switch crdtType {
	case proto.CRDTType_COUNTER:
		tmpUpd = Increment{}.FromUpdateObject(protobuf)
	case proto.CRDTType_LWWREG:
		tmpUpd = SetValue{}.FromUpdateObject(protobuf)
	case proto.CRDTType_ORSET:
		tmpUpd = updateSetProtoToAntidoteUpdate(protobuf)
	case proto.CRDTType_ORMAP:
		tmpUpd = updateMapProtoToAntidoteUpdate(protobuf)
	case proto.CRDTType_RRMAP:
		tmpUpd = updateEmbMapProtoToAntidoteUpdate(protobuf)
	case proto.CRDTType_TOPK_RMV:
		tmpUpd = updateTopkProtoToAntidoteUpdate(protobuf)
	case proto.CRDTType_AVG:
		tmpUpd = AddMultipleValue{}.FromUpdateObject(protobuf)
	case proto.CRDTType_MAXMIN:
		tmpUpd = updateMaxMinProtoToAntidoteUpdate(protobuf)
	case proto.CRDTType_TOPSUM:
		tmpUpd = updateTopsProtoToAntidoteUpdate(protobuf)
	}

	return &tmpUpd
}

func PartialReadOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs, crdtType proto.CRDTType, readType proto.READType) (read *ReadArguments) {
	var tmpRead ReadArguments = nil

	switch readType {
	case proto.READType_FULL:
		tmpRead = StateReadArguments{}

	//Set
	case proto.READType_LOOKUP:
		tmpRead = LookupReadArguments{}.FromPartialRead(protobuf)
		tmpRead = LookupReadArguments{Elem: Element(protobuf.GetSet().GetLookup().GetElement())}

	//Maps
	case proto.READType_HAS_KEY:
		tmpRead = HasKeyArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_KEYS:
		tmpRead = GetKeysArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_VALUE:
		tmpRead = partialGetValueOpToAntidoteRead(protobuf, crdtType)
	case proto.READType_GET_VALUES:
		tmpRead = partialGetValuesOpToAntidoteRead(protobuf, crdtType)

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
	}

	return &tmpRead
}

func ReadRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType, readType proto.READType) (state State) {
	//ConvertProtoObjectToAntidoteState
	if readType != proto.READType_FULL {
		return partialReadRespProtoToAntidoteState(protobuf, crdtType, readType)
	}

	switch crdtType {
	case proto.CRDTType_COUNTER:
		state = CounterState{}.FromReadResp(protobuf)
	case proto.CRDTType_LWWREG:
		state = RegisterState{}.FromReadResp(protobuf)
	case proto.CRDTType_ORSET:
		state = SetAWValueState{}.FromReadResp(protobuf)
	case proto.CRDTType_ORMAP:
		state = MapEntryState{}.FromReadResp(protobuf)
	case proto.CRDTType_RRMAP:
		state = EmbMapEntryState{}.FromReadResp(protobuf)
	case proto.CRDTType_TOPK_RMV, proto.CRDTType_TOPSUM:
		state = TopKValueState{}.FromReadResp(protobuf)
	case proto.CRDTType_AVG:
		state = AvgState{}.FromReadResp(protobuf)
	case proto.CRDTType_MAXMIN:
		state = MaxMinState{}.FromReadResp(protobuf)
		//case proto.CRDTType_TOPSUM:
		//state = TopSValueState{}.FromReadResp(protobuf)
	}

	return
}

func partialReadRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType, readType proto.READType) (state State) {
	switch readType {
	//Sets
	case proto.READType_LOOKUP:
		state = SetAWLookupState{}.FromReadResp(protobuf)

	//Maps
	case proto.READType_HAS_KEY:
		state = partialHasKeyRespProtoToAntidoteState(protobuf, crdtType)
	case proto.READType_GET_KEYS:
		state = partialGetKeysRespProtoToAntidoteState(protobuf, crdtType)
	case proto.READType_GET_VALUE:
		state = partialGetValueRespProtoToAntidoteState(protobuf, crdtType)
	case proto.READType_GET_VALUES:
		state = partialGetValuesRespProtoToAntidoteState(protobuf, crdtType)

	//Topk
	case proto.READType_GET_N, proto.READType_GET_ABOVE_VALUE:
		//state = TopKValueState{}.FromReadResp(protobuf)
		state = partialTopRespProtoToAntidoteState(protobuf, crdtType)

	//Avg
	case proto.READType_GET_FULL_AVG:
		state = AvgFullState{}.FromReadResp(protobuf)
	}

	return
}

func DownstreamProtoToAntidoteDownstream(protobuf *proto.ProtoOpDownstream, crdtType proto.CRDTType) (downOp DownstreamArguments) {
	switch crdtType {
	case proto.CRDTType_COUNTER:
		downOp = downstreamProtoCounterToAntidoteDownstream(protobuf)
	case proto.CRDTType_LWWREG:
		downOp = DownstreamSetValue{}.FromReplicatorObj(protobuf)
	case proto.CRDTType_ORSET:
		downOp = downstreamProtoSetToAntidoteDownstream(protobuf)
	case proto.CRDTType_ORMAP:
		downOp = downstreamProtoORMapToAntidoteDownstream(protobuf)
	case proto.CRDTType_RRMAP:
		downOp = downstreamProtoRRMapToAntidoteDownstream(protobuf)
	case proto.CRDTType_TOPK_RMV:
		downOp = downstreamProtoTopKToAntidoteDownstream(protobuf)
	case proto.CRDTType_AVG:
		downOp = AddMultipleValue{}.FromReplicatorObj(protobuf)
	case proto.CRDTType_MAXMIN:
		downOp = downstreamProtoMaxMinToAntidoteDownstream(protobuf)
	case proto.CRDTType_TOPSUM:
		//downOp = DownstreamTopSAdd{}.FromReplicatorObj(protobuf)
		downOp = downstreamProtoTopSToAntidoteDownstream(protobuf)
	}

	return
}

func StateProtoToCrdt(protobuf *proto.ProtoState, crdtType proto.CRDTType, ts *clocksi.Timestamp, replicaID int16) (crdt CRDT) {
	switch crdtType {
	case proto.CRDTType_COUNTER:
		crdt = (&CounterCrdt{}).FromProtoState(protobuf, ts, replicaID)
	case proto.CRDTType_LWWREG:
		crdt = (&LwwRegisterCrdt{}).FromProtoState(protobuf, ts, replicaID)
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
		return EmbMapUpdateAll{}.FromUpdateObject(protobuf)
	}
	return MapRemoveAll{}.FromUpdateObject(protobuf)
}

func updateTopkProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
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

func updateMaxMinProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if protobuf.GetMaxminop().GetIsMax() {
		return MaxAddValue{}.FromUpdateObject(protobuf)
	}
	return MinAddValue{}.FromUpdateObject(protobuf)
}

func updateTopsProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	adds := protobuf.GetTopkrmvop().GetAdds()
	if len(adds) == 1 {
		if adds[0].GetScore() >= 0 {
			return TopSAdd{}.FromUpdateObject(protobuf)
		} else {
			return TopSSub{}.FromUpdateObject(protobuf)
		}
	} else if adds[0].GetScore() >= 0 {
		return TopSAddAll{}.FromUpdateObject(protobuf)
	}
	return TopSSubAll{}.FromUpdateObject(protobuf)
}

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
	return EmbMapEntryState{}.FromReadResp(protobuf)
}

func partialTopRespProtoToAntidoteState(protobuf *proto.ApbReadObjectResp, crdtType proto.CRDTType) (state State) {
	if crdtType == proto.CRDTType_TOPK_RMV {
		return TopKValueState{}.FromReadResp(protobuf)
	}
	return TopSValueState{}.FromReadResp(protobuf)
}

func partialGetValueOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs, crdtType proto.CRDTType) (readArgs ReadArguments) {
	if crdtType == proto.CRDTType_ORMAP {
		return GetValueArguments{}.FromPartialRead(protobuf)
	}
	return EmbMapGetValueArguments{}.FromPartialRead(protobuf)
}

func partialGetValuesOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs, crdtType proto.CRDTType) (readArgs ReadArguments) {
	if crdtType == proto.CRDTType_ORMAP {
		return GetValuesArguments{}.FromPartialRead(protobuf)
	}
	return EmbMapPartialArguments{}.FromPartialRead(protobuf)
}

func downstreamProtoCounterToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if protobuf.GetCounterOp().GetIsInc() {
		return Increment{}.FromReplicatorObj(protobuf)
	}
	return Decrement{}.FromReplicatorObj(protobuf)
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
		return DownstreamRWEmbMapUpdateAll{}.FromReplicatorObj(protobuf)
	}
	return DownstreamRWEmbMapRemoveAll{}.FromReplicatorObj(protobuf)
}

func downstreamProtoTopKToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
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

func createMapNestedOps(upds map[string]UpdateArguments) (converted []*proto.ApbMapNestedUpdate) {
	converted = make([]*proto.ApbMapNestedUpdate, len(upds))
	i := 0
	for key, upd := range upds {
		crdtType := proto.CRDTType(upd.GetCRDTType())
		converted[i] = &proto.ApbMapNestedUpdate{
			Key:    &proto.ApbMapKey{Key: []byte(key), Type: &crdtType},
			Update: upd.(ProtoUpd).ToUpdateObject(),
		}
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
		protos[i] = &proto.ProtoORMapRemove{Key: []byte(key), Elems: protoElems}
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
