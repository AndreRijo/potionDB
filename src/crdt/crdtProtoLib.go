package crdt

import (
	"clocksi"
	"proto"

	pb "github.com/golang/protobuf/proto"
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
	CRDT SPECIFIC FUNCS
		COUNTER
		REGISTER
		SET
		MAP
		EMBMAP
		TOPK
		AVG
		MAXMIN
	MISCELANEOUS
*/

//TODO: Move each state/op to its respective file
//TODO: Maybe think of some way to avoid requiring the generic methods?
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

/*****GLOBAL FUNCS*****/
func UpdateProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation, crdtType proto.CRDTType) (op *UpdateArguments) {
	var tmpUpd UpdateArguments = &NoOp{}

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
	default:
		//TODO: Support other types and error case, and return error to client
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

	//Topk
	case proto.READType_GET_N:
		tmpRead = GetTopNArguments{}.FromPartialRead(protobuf)
	case proto.READType_GET_ABOVE_VALUE:
		tmpRead = GetTopKAboveValueArguments{}.FromPartialRead(protobuf)
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
	case proto.CRDTType_TOPK_RMV:
		state = TopKValueState{}.FromReadResp(protobuf)
	case proto.CRDTType_AVG:
		state = AvgState{}.FromReadResp(protobuf)
	case proto.CRDTType_MAXMIN:
		state = MaxMinState{}.FromReadResp(protobuf)
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
		state = TopKValueState{}.FromReadResp(protobuf)
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
	}

	return
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
	if len(protobuf.GetTopkrmvop().GetAdds()) > 0 {
		return TopKAdd{}.FromUpdateObject(protobuf)
	}
	return TopKRemove{}.FromUpdateObject(protobuf)
}

func updateMaxMinProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	if protobuf.GetMaxminop().GetIsMax() {
		return MaxAddValue{}.FromUpdateObject(protobuf)
	}
	return MinAddValue{}.FromUpdateObject(protobuf)
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

/*
case proto.CRDTType_COUNTER:
		downOp = downstreamProtoCounterToAntidoteDownstream(protobuf)
	case proto.CRDTType_LWWREG:
		downOp = SetValue{}.FromReplicatorObj(protobuf)
	case proto.CRDTType_ORSET:
		downOp = downstreamProtoSetToAntidoteDownstream(protobuf)
	case proto.CRDTType_ORMAP:
		downOp = downstreamProtoORMapToAntidoteDownstream(protobuf)
	case proto.CRDTType_RRMAP:
		downOp = downstreamProtoRRMapToAntidoteDownstream(protobuf)
	case proto.CRDTType_TOPK_RMV:
		downOp = downstreamProtoTopKToAntidoteDownstream(protobuf)
	case proto.CRDTType_AVG:
		downOp = downstreamProtoAvgToAntidoteDownstream(protobuf)
	case proto.CRDTType_MAXMIN:
		downOp = downstreamProtoMaxMinToAntidoteDownstream(protobuf)

func downstreamProtoCounterToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArgs) {

	return
}
*/
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
		return DownstreamTopKAdd{}.FromReplicatorObj(protobuf)
	}
	return DownstreamTopKRemove{}.FromReplicatorObj(protobuf)
}

func downstreamProtoMaxMinToAntidoteDownstream(protobuf *proto.ProtoOpDownstream) (downOp DownstreamArguments) {
	if max := protobuf.GetMaxminOp().GetMax(); max != nil {
		return MaxAddValue{}.FromReplicatorObj(protobuf)
	}
	return MinAddValue{}.FromReplicatorObj(protobuf)
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

/*****CRDT SPECIFIC FUNCS*****/

/***COUNTER***/
func (crdtOp Increment) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Change = int32(protobuf.GetCounterop().GetInc())
	return crdtOp
}

func (crdtOp Increment) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Counterop: &proto.ApbCounterUpdate{Inc: pb.Int64(int64(crdtOp.Change))}}
}

func (crdtOp Decrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Change = int32(-protobuf.GetCounterop().GetInc())
	return crdtOp
}

func (crdtOp Decrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Counterop: &proto.ApbCounterUpdate{Inc: pb.Int64(int64(-crdtOp.Change))}}
}

func (crdtState CounterState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Value = protobuf.GetCounter().GetValue()
	return crdtState
}

func (crdtState CounterState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Counter: &proto.ApbGetCounterResp{Value: pb.Int32(crdtState.Value)}}
}

func (downOp Increment) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Change = protobuf.GetCounterOp().GetChange()
	return downOp
}

func (downOp Decrement) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Change = -protobuf.GetCounterOp().GetChange()
	return downOp
}

func (downOp Increment) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{CounterOp: &proto.ProtoCounterDownstream{IsInc: pb.Bool(true), Change: pb.Int32(downOp.Change)}}
}

func (downOp Decrement) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{CounterOp: &proto.ProtoCounterDownstream{IsInc: pb.Bool(false), Change: pb.Int32(downOp.Change)}}
}

/***REGISTER***/
func (crdtOp SetValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.NewValue = string(protobuf.GetRegop().GetValue())
	return crdtOp
}

func (crdtOp SetValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Regop: &proto.ApbRegUpdate{Value: []byte(crdtOp.NewValue.(string))}}
}

func (crdtState RegisterState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Value = string(protobuf.GetReg().GetValue())
	return crdtState
}

func (crdtState RegisterState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Reg: &proto.ApbGetRegResp{Value: []byte((crdtState.Value).(string))}}
}

func (downOp DownstreamSetValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	regOp := protobuf.GetLwwregOp()
	downOp.NewValue, downOp.ReplicaID, downOp.Ts = regOp.GetValue(), int16(regOp.GetReplicaID()), regOp.GetTs()
	return downOp
}

func (downOp DownstreamSetValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{LwwregOp: &proto.ProtoLWWRegisterDownstream{
		Value: []byte(downOp.NewValue.(string)), Ts: pb.Int64(downOp.Ts), ReplicaID: pb.Int32(int32(downOp.ReplicaID)),
	}}
}

/***SET***/
func (crdtOp AddAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Elems = ByteMatrixToElementArray(protobuf.GetSetop().GetAdds())
	return crdtOp
}

func (crdtOp AddAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	opType := proto.ApbSetUpdate_ADD
	elements := ElementArrayToByteMatrix(crdtOp.Elems)
	return &proto.ApbUpdateOperation{Setop: &proto.ApbSetUpdate{Optype: &opType, Adds: elements}}
}

func (crdtOp RemoveAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Elems = ByteMatrixToElementArray(protobuf.GetSetop().GetRems())
	return crdtOp
}

func (crdtOp RemoveAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	opType := proto.ApbSetUpdate_REMOVE
	elements := ElementArrayToByteMatrix(crdtOp.Elems)
	return &proto.ApbUpdateOperation{Setop: &proto.ApbSetUpdate{Optype: &opType, Rems: elements}}
}

func (crdtOp Add) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Element = Element(protobuf.GetSetop().GetAdds()[0])
	return crdtOp
}

func (crdtOp Add) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	opType := proto.ApbSetUpdate_ADD
	element := [][]byte{[]byte(crdtOp.Element)}
	return &proto.ApbUpdateOperation{Setop: &proto.ApbSetUpdate{Optype: &opType, Adds: element}}
}

func (crdtOp Remove) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Element = Element(protobuf.GetSetop().GetRems()[0])
	return crdtOp
}

func (crdtOp Remove) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	opType := proto.ApbSetUpdate_REMOVE
	element := [][]byte{[]byte(crdtOp.Element)}
	return &proto.ApbUpdateOperation{Setop: &proto.ApbSetUpdate{Optype: &opType, Rems: element}}
}

func (crdtState SetAWValueState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Elems = ByteMatrixToElementArray(protobuf.GetSet().GetValue())
	return crdtState
}

func (crdtState SetAWValueState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Set: &proto.ApbGetSetResp{Value: ElementArrayToByteMatrix(crdtState.Elems)}}
}

func (crdtState SetAWLookupState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.HasElem = protobuf.GetPartread().GetSet().GetLookup().GetHas()
	return crdtState
}

func (crdtState SetAWLookupState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Set: &proto.ApbSetPartialReadResp{
		Lookup: &proto.ApbSetLookupReadResp{Has: pb.Bool(crdtState.HasElem)},
	}}}
}

func (args LookupReadArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.Elem = Element(protobuf.GetSet().GetLookup().GetElement())
	return args
}

func (args LookupReadArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Set: &proto.ApbSetPartialRead{Lookup: &proto.ApbSetLookupRead{Element: []byte(args.Elem)}}}
}

func (downOp DownstreamAddAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	adds := protobuf.GetSetOp().GetAdds()
	downOp.Elems = make(map[Element]Unique)
	for _, pairProto := range adds {
		downOp.Elems[Element(pairProto.GetValue())] = Unique(pairProto.GetUnique())
	}
	return downOp
}

func (downOp DownstreamRemoveAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	rems := protobuf.GetSetOp().GetRems()
	downOp.Elems = make(map[Element]UniqueSet)
	for _, pairProto := range rems {
		downOp.Elems[Element(pairProto.GetValue())] = UInt64ArrayToUniqueSet(pairProto.GetUniques())
	}
	return downOp
}

func (downOp DownstreamAddAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	adds := make([]*proto.ProtoValueUnique, len(downOp.Elems))
	i := 0
	for value, unique := range downOp.Elems {
		adds[i] = &proto.ProtoValueUnique{Value: []byte(value), Unique: pb.Uint64(uint64(unique))}
		i++
	}
	return &proto.ProtoOpDownstream{SetOp: &proto.ProtoSetDownstream{Adds: adds}}
}

func (downOp DownstreamRemoveAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	rems := make([]*proto.ProtoValueUniques, len(downOp.Elems))
	i := 0
	for value, uniques := range downOp.Elems {
		rems[i] = &proto.ProtoValueUniques{Value: []byte(value), Uniques: UniqueSetToUInt64Array(uniques)}
		i++
	}
	return &proto.ProtoOpDownstream{SetOp: &proto.ProtoSetDownstream{Rems: rems}}
}

/***MAP***/
func (crdtOp MapAddAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Values = make(map[string]Element)
	protoAdds := protobuf.GetMapop().GetUpdates()
	for _, mapUpd := range protoAdds {
		crdtOp.Values[string(mapUpd.GetKey().GetKey())] =
			Element(mapUpd.GetUpdate().GetRegop().GetValue())
	}
	return crdtOp
}

func (crdtOp MapAddAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Mapop: &proto.ApbMapUpdate{
		Updates:     mapEntriesToProto(crdtOp.Values),
		RemovedKeys: []*proto.ApbMapKey{},
	}}
}

func (crdtOp MapRemoveAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	protoRems := protobuf.GetMapop().GetRemovedKeys()
	crdtOp.Keys = make([]string, len(protoRems))
	i := 0
	for _, mapKey := range protoRems {
		crdtOp.Keys[i] = string(mapKey.GetKey())
		i++
	}
	return crdtOp
}

func (crdtOp MapRemoveAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Mapop: &proto.ApbMapUpdate{
		Updates:     []*proto.ApbMapNestedUpdate{},
		RemovedKeys: stringArrayToMapKeyArray(crdtOp.Keys),
	}}
}

func (crdtOp MapAdd) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	protoAdd := protobuf.GetMapop().GetUpdates()[0]
	crdtOp.Key, crdtOp.Value = string(protoAdd.GetKey().GetKey()), Element(protoAdd.GetUpdate().GetRegop().GetValue())
	return crdtOp
}

func (crdtOp MapAdd) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Mapop: &proto.ApbMapUpdate{
		Updates:     mapEntriesToProto(map[string]Element{crdtOp.Key: crdtOp.Value}),
		RemovedKeys: []*proto.ApbMapKey{},
	}}
}

func (crdtOp MapRemove) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Key = string(protobuf.GetMapop().GetRemovedKeys()[0].GetKey())
	return crdtOp
}

func (crdtOp MapRemove) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Mapop: &proto.ApbMapUpdate{
		Updates:     []*proto.ApbMapNestedUpdate{},
		RemovedKeys: stringArrayToMapKeyArray([]string{crdtOp.Key}),
	}}
}

func (crdtState MapEntryState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Values = make(map[string]Element)
	if entries := protobuf.GetMap().GetEntries(); entries != nil {
		for _, entry := range entries {
			crdtState.Values[string(entry.GetKey().GetKey())] = Element(entry.GetValue().GetReg().GetValue())
		}
	} else {
		//Partial read
		protoResp := protobuf.GetPartread().GetMap().GetGetvalues()
		protoKeys, protoValues := protoResp.GetKeys(), protoResp.GetValues()
		for i, value := range protoValues {
			crdtState.Values[string(protoKeys[i])] = Element(value.GetValue().GetReg().GetValue())
		}
	}

	return crdtState
}

func (crdtState MapEntryState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Map: &proto.ApbGetMapResp{Entries: entriesToApbMapEntries(crdtState.Values)}}
}

func (crdtState MapHasKeyState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.HasKey = protobuf.GetPartread().GetMap().GetHaskey().GetHas()
	return crdtState
}

func (crdtState MapHasKeyState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
		Haskey: &proto.ApbMapHasKeyReadResp{Has: pb.Bool(crdtState.HasKey)},
	}}}
}

func (crdtState MapKeysState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Keys = protobuf.GetPartread().GetMap().GetGetkeys().GetKeys()
	return crdtState
}

func (crdtState MapKeysState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
		Getkeys: &proto.ApbMapGetKeysReadResp{Keys: crdtState.Keys},
	}}}
}

func (crdtState MapGetValueState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Value = Element(protobuf.GetPartread().GetMap().GetGetvalue().GetValue().GetReg().GetValue())
	return crdtState
}

func (crdtState MapGetValueState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
		Getvalue: &proto.ApbMapGetValueResp{Value: &proto.ApbReadObjectResp{
			Reg: &proto.ApbGetRegResp{Value: []byte(crdtState.Value)},
		}},
	}}}
}

func (args HasKeyArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.Key = string(protobuf.GetMap().GetHaskey().GetKey())
	return args
}

func (args GetKeysArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args GetValueArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.Key = string(protobuf.GetMap().GetGetvalue().GetKey())
	return args
}

func (args GetValuesArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.Keys = byteArrayToStringArray(protobuf.GetMap().GetGetvalues().GetKeys())
	return args
}

func (args HasKeyArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Map: &proto.ApbMapPartialRead{Haskey: &proto.ApbMapHasKeyRead{Key: []byte(args.Key)}}}
}

func (args GetKeysArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Map: &proto.ApbMapPartialRead{Getkeys: &proto.ApbMapGetKeysRead{}}}
}

func (args GetValueArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Map: &proto.ApbMapPartialRead{Getvalue: &proto.ApbMapGetValueRead{Key: []byte(args.Key)}}}
}

func (args GetValuesArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	keys := stringArrayToByteArray(args.Keys)
	return &proto.ApbPartialReadArgs{Map: &proto.ApbMapPartialRead{Getvalues: &proto.ApbMapGetValuesRead{Keys: keys}}}
}

func (downOp DownstreamORMapAddAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	setProto := protobuf.GetOrmapOp()
	downOp.Rems = createORMapDownRems(setProto.GetRems())

	downOp.Adds = make(map[string]UniqueElemPair)
	addsProto := setProto.GetAdds()
	for _, addProto := range addsProto {
		downOp.Adds[string(addProto.GetKey())] = UniqueElemPair{Element: Element(addProto.GetElement()), Unique: Unique(addProto.GetUnique())}
	}
	return downOp
}

func (downOp DownstreamORMapRemoveAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Rems = createORMapDownRems(protobuf.GetOrmapOp().GetRems())
	return downOp
}

func (downOp DownstreamORMapAddAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	adds := make([]*proto.ProtoKeyValueUnique, len(downOp.Adds))
	i := 0
	for key, pair := range downOp.Adds {
		adds[i] = &proto.ProtoKeyValueUnique{
			Key: []byte(key), Element: []byte(pair.Element), Unique: pb.Uint64(uint64(pair.Unique)),
		}
		i++
	}
	return &proto.ProtoOpDownstream{OrmapOp: &proto.ProtoORMapDownstream{Adds: adds, Rems: createProtoMapRemoves(downOp.Rems)}}
}

func (downOp DownstreamORMapRemoveAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{OrmapOp: &proto.ProtoORMapDownstream{Rems: createProtoMapRemoves(downOp.Rems)}}
}

/***EMBMAP***/
//Note: the states/ops common with OrMap are defined there

func (crdtOp EmbMapUpdateAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	protoAdds := protobuf.GetMapop().GetUpdates()
	crdtOp.Upds = make(map[string]UpdateArguments)
	for _, mapUpd := range protoAdds {
		crdtOp.Upds[string(mapUpd.GetKey().GetKey())] =
			*UpdateProtoToAntidoteUpdate(mapUpd.GetUpdate(), mapUpd.GetKey().GetType())
	}
	return crdtOp
}

func (crdtOp EmbMapUpdateAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Mapop: &proto.ApbMapUpdate{
		Updates:     createMapNestedOps(crdtOp.Upds),
		RemovedKeys: []*proto.ApbMapKey{},
	}}
}

func (crdtOp EmbMapUpdate) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	protoAdd := protobuf.GetMapop().GetUpdates()[0]
	crdtOp.Key = string(protoAdd.GetKey().GetKey())
	crdtOp.Upd = *UpdateProtoToAntidoteUpdate(protoAdd.GetUpdate(), protoAdd.GetKey().GetType())
	return crdtOp
}

func (crdtOp EmbMapUpdate) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Mapop: &proto.ApbMapUpdate{
		Updates:     createMapNestedOps(map[string]UpdateArguments{crdtOp.Key: crdtOp.Upd}),
		RemovedKeys: []*proto.ApbMapKey{},
	}}
}

func (crdtState EmbMapEntryState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.States = make(map[string]State)

	if entries := protobuf.GetMap().GetEntries(); entries != nil {
		var pKey *proto.ApbMapKey
		for _, entry := range entries {
			pKey = entry.GetKey()
			//In this case the inner reads are also full read for sure. Might be worth changing this in the future though.
			crdtState.States[string(pKey.GetKey())] = ReadRespProtoToAntidoteState(entry.GetValue(), pKey.GetType(), proto.READType_FULL)
		}
	} else {
		//Partial read
		mapProto := protobuf.GetPartread().GetMap().GetGetvalues()
		protoKeys, protoValues := mapProto.GetKeys(), mapProto.GetValues()
		for i, protoValue := range protoValues {
			innerCrdtType, innerReadType := protoValue.GetCrdttype(), protoValue.GetParttype()
			crdtState.States[string(protoKeys[i])] = ReadRespProtoToAntidoteState(protoValue.GetValue(), innerCrdtType, innerReadType)
		}
	}

	return crdtState
}

func (crdtState EmbMapEntryState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Map: &proto.ApbGetMapResp{Entries: crdtsToApbMapEntries(crdtState.States)}}
}

func (crdtState EmbMapHasKeyState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.HasKey = protobuf.GetPartread().GetMap().GetHaskey().GetHas()
	return crdtState
}

func (crdtState EmbMapHasKeyState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
		Haskey: &proto.ApbMapHasKeyReadResp{Has: pb.Bool(crdtState.HasKey)},
	}}}
}

func (crdtState EmbMapKeysState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Keys = protobuf.GetPartread().GetMap().GetGetkeys().GetKeys()
	return crdtState
}

func (crdtState EmbMapKeysState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
		Getkeys: &proto.ApbMapGetKeysReadResp{Keys: crdtState.Keys},
	}}}
}

func (crdtState EmbMapGetValueState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	mapProto := protobuf.GetPartread().GetMap().GetGetvalue()
	innerCrdtType, innerReadType := mapProto.GetCrdttype(), mapProto.GetParttype()
	crdtState.State = ReadRespProtoToAntidoteState(mapProto.GetValue(), innerCrdtType, innerReadType)
	return crdtState
}

func (crdtState EmbMapGetValueState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	innerCrdtType, innerReadType := crdtState.State.GetCRDTType(), crdtState.State.GetREADType()
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
		Getvalue: &proto.ApbMapGetValueResp{
			Value: crdtState.State.(ProtoState).ToReadResp(), Crdttype: &innerCrdtType, Parttype: &innerReadType,
		},
	}}}
}

func (args EmbMapGetValueArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	getValueProto := protobuf.GetMap().GetGetvalue()
	protoArgs := getValueProto.GetArgs()
	if protoArgs == nil {
		//Read whole inner state
		return EmbMapGetValueArguments{Key: string(getValueProto.GetKey()), Args: StateReadArguments{}}
	}
	//Read part of inner state
	innerCrdtType, innerReadType := protoArgs.GetType(), protoArgs.GetReadtype()
	args.Key = string(getValueProto.GetKey())
	args.Args = *PartialReadOpToAntidoteRead(protoArgs.GetArgs(), innerCrdtType, innerReadType)
	return args
}

func (args EmbMapPartialArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.Args = make(map[string]ReadArguments)
	getValuesProto := protobuf.GetMap().GetGetvalues()
	byteKeys, argsProto := getValuesProto.GetKeys(), getValuesProto.GetArgs()

	for i, argProto := range argsProto {
		args.Args[string(byteKeys[i])] = *PartialReadOpToAntidoteRead(argProto.GetArgs(), argProto.GetType(), argProto.GetReadtype())
	}
	return args
}

func (args EmbMapGetValueArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Map: &proto.ApbMapPartialRead{Getvalue: &proto.ApbMapGetValueRead{
		Key:  []byte(args.Key),
		Args: createMapGetValuesRead(args.Args),
	}}}
}

func (args EmbMapPartialArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	keys := make([][]byte, len(args.Args))
	argsProtos := make([]*proto.ApbMapEmbPartialArgs, len(args.Args))
	i := 0
	for key, arg := range args.Args {
		keys[i] = []byte(key)
		argsProtos[i] = createMapGetValuesRead(arg)
		i++
	}

	return &proto.ApbPartialReadArgs{Map: &proto.ApbMapPartialRead{Getvalues: &proto.ApbMapGetValuesRead{Keys: keys, Args: argsProtos}}}
}

func (downOp DownstreamRWEmbMapUpdateAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	adds := protobuf.GetRwembmapOp().GetAdds()
	upds, clk := adds.GetUpds(), adds.GetVc()
	downOp.Upds, downOp.RmvEntries = make(map[string]DownstreamArguments), make(map[int16]int64)

	for _, upd := range upds {
		downOp.Upds[string(upd.GetKey())] = DownstreamProtoToAntidoteDownstream(upd.GetUpd(), upd.GetType())
	}
	for _, protoEntry := range clk {
		downOp.RmvEntries[int16(protoEntry.GetSenderID())] = protoEntry.GetReplicaTs()
	}
	return downOp
}

func (downOp DownstreamRWEmbMapRemoveAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	rems := protobuf.GetRwembmapOp().GetRems()
	downOp.Rems, downOp.Ts, downOp.ReplicaID = rems.GetKeys(), rems.GetTs(), int16(rems.GetReplicaID())
	return downOp
}

func (downOp DownstreamRWEmbMapUpdateAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	upds, vc := make([]*proto.ProtoEmbMapUpd, len(downOp.Upds)), make([]*proto.ProtoStableClock, len(downOp.RmvEntries))
	i, j := 0, 0
	for key, upd := range downOp.Upds {
		crdtType := upd.GetCRDTType()
		upds[i] = &proto.ProtoEmbMapUpd{Key: []byte(key), Type: &crdtType, Upd: upd.(ProtoDownUpd).ToReplicatorObj()}
		i++
	}
	for key, clk := range downOp.RmvEntries {
		vc[j] = &proto.ProtoStableClock{SenderID: pb.Int32(int32(key)), ReplicaTs: pb.Int64(clk)}
		j++
	}
	return &proto.ProtoOpDownstream{RwembmapOp: &proto.ProtoRWEmbMapDownstream{
		Adds: &proto.ProtoRWEmbMapUpdates{Upds: upds, Vc: vc, ReplicaID: pb.Int32(int32(downOp.ReplicaID))},
	}}
}

func (downOp DownstreamRWEmbMapRemoveAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{RwembmapOp: &proto.ProtoRWEmbMapDownstream{
		Rems: &proto.ProtoRWEmbMapRemoves{Keys: downOp.Rems, ReplicaID: pb.Int32(int32(downOp.ReplicaID)), Ts: pb.Int64(downOp.Ts)},
	}}
}

/***TOPK***/
func (crdtOp TopKAdd) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	add := protobuf.GetTopkrmvop().GetAdds()[0]
	crdtOp.TopKScore = TopKScore{Id: add.GetPlayerId(), Score: add.GetScore()}
	return crdtOp
}

func (crdtOp TopKAdd) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Topkrmvop: &proto.ApbTopkRmvUpdate{
		Adds: []*proto.ApbIntPair{&proto.ApbIntPair{PlayerId: pb.Int32(crdtOp.Id), Score: pb.Int32(crdtOp.Score)}},
	}}
}

func (crdtOp TopKRemove) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Id = protobuf.GetTopkrmvop().GetRems()[0]
	return crdtOp
}

func (crdtOp TopKRemove) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Topkrmvop: &proto.ApbTopkRmvUpdate{Rems: []int32{crdtOp.Id}}}
}

func (crdtState TopKValueState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	protoScores := protobuf.GetTopk().GetValues()
	if protoScores == nil {
		//Partial read
		protoScores = protobuf.GetPartread().GetTopk().GetPairs().GetValues()
	}
	crdtState.Scores = make([]TopKScore, len(protoScores))
	for i, pair := range protoScores {
		crdtState.Scores[i] = TopKScore{Id: pair.GetPlayerId(), Score: pair.GetScore()}
	}
	return crdtState
}

func (crdtState TopKValueState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	protos := make([]*proto.ApbIntPair, len(crdtState.Scores))
	for i, score := range crdtState.Scores {
		protos[i] = &proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(score.Score)}
	}
	return &proto.ApbReadObjectResp{Topk: &proto.ApbGetTopkResp{Values: protos}}
}

func (args GetTopNArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.NumberEntries = protobuf.GetTopk().GetGetn().GetAmount()
	return args
}

func (args GetTopKAboveValueArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.MinValue = protobuf.GetTopk().GetGetabovevalue().GetMinValue()
	return args
}

func (args GetTopNArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Topk: &proto.ApbTopkPartialRead{Getn: &proto.ApbTopkGetNRead{
		Amount: pb.Int32(args.NumberEntries),
	}}}
}

func (args GetTopKAboveValueArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Topk: &proto.ApbTopkPartialRead{Getabovevalue: &proto.ApbTopkAboveValueRead{
		MinValue: pb.Int32(args.MinValue),
	}}}
}

func (downOp DownstreamTopKAdd) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	addProto := protobuf.GetTopkrmvOp().GetAdds()[0]
	downOp.Id, downOp.Score, downOp.Ts, downOp.ReplicaID = addProto.GetId(), addProto.GetScore(), addProto.GetTs(), int16(addProto.GetReplicaID())
	return downOp
}

func (downOp DownstreamTopKRemove) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	remProto := protobuf.GetTopkrmvOp().GetRems()[0]
	downOp.Id, downOp.Vc = remProto.GetId(), clocksi.ClockSiTimestamp{}.FromBytes(remProto.GetVc())
	return downOp
}

func (downOp DownstreamTopKAdd) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{TopkrmvOp: &proto.ProtoTopKRmvDownstream{Adds: []*proto.ProtoTopKElement{&proto.ProtoTopKElement{
		Id: pb.Int32(downOp.Id), Score: pb.Int32(downOp.Score), Ts: pb.Int64(downOp.Ts), ReplicaID: pb.Int32(int32(downOp.ReplicaID)),
	}}}}
}

func (downOp DownstreamTopKRemove) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{TopkrmvOp: &proto.ProtoTopKRmvDownstream{Rems: []*proto.ProtoTopKIdVc{&proto.ProtoTopKIdVc{
		Id: pb.Int32(downOp.Id), Vc: downOp.Vc.ToBytes(),
	}}}}
}

/***AVG***/
func (crdtOp AddMultipleValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	avgProto := protobuf.GetAvgop()
	crdtOp.SumValue, crdtOp.NAdds = avgProto.GetValue(), avgProto.GetNValues()
	return crdtOp
}

func (crdtOp AddMultipleValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Avgop: &proto.ApbAverageUpdate{
		Value: pb.Int64(crdtOp.SumValue), NValues: pb.Int64(crdtOp.NAdds),
	}}
}

func (crdtOp AddValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Value = protobuf.GetAvgop().GetValue()
	return crdtOp
}

func (crdtOp AddValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Avgop: &proto.ApbAverageUpdate{Value: pb.Int64(crdtOp.Value), NValues: pb.Int64(1)}}
}

func (crdtState AvgState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Value = protobuf.GetAvg().GetAvg()
	return crdtState
}

func (crdtState AvgState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Avg: &proto.ApbGetAverageResp{Avg: pb.Float64(crdtState.Value)}}
}

func (downOp AddMultipleValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	avgProto := protobuf.GetAvgOp()
	downOp.SumValue, downOp.NAdds = avgProto.GetSumValue(), avgProto.GetNAdds()
	return downOp
}

func (downOp AddMultipleValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{AvgOp: &proto.ProtoAvgDownstream{SumValue: pb.Int64(downOp.SumValue), NAdds: pb.Int64(downOp.NAdds)}}
}

/***MAXMIN***/
func (crdtOp MaxAddValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Value = protobuf.GetMaxminop().GetValue()
	return crdtOp
}

func (crdtOp MaxAddValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Maxminop: &proto.ApbMaxMinUpdate{Value: pb.Int64(crdtOp.Value), IsMax: pb.Bool(true)}}
}

func (crdtOp MinAddValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Value = protobuf.GetMaxminop().GetValue()
	return crdtOp
}

func (crdtOp MinAddValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Maxminop: &proto.ApbMaxMinUpdate{Value: pb.Int64(crdtOp.Value), IsMax: pb.Bool(false)}}
}

func (crdtState MaxMinState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Value = protobuf.GetMaxmin().GetValue()
	return crdtState
}

func (crdtState MaxMinState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Maxmin: &proto.ApbGetMaxMinResp{Value: pb.Int64(crdtState.Value)}}
}

func (downOp MaxAddValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Value = protobuf.GetMaxminOp().GetMax().GetValue()
	return downOp
}

func (downOp MinAddValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Value = protobuf.GetMaxminOp().GetMin().GetValue()
	return downOp
}

func (downOp MaxAddValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{MaxminOp: &proto.ProtoMaxMinDownstream{Max: &proto.ProtoMaxDownstream{Value: pb.Int64(downOp.Value)}}}
}

func (downOp MinAddValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{MaxminOp: &proto.ProtoMaxMinDownstream{Min: &proto.ProtoMinDownstream{Value: pb.Int64(downOp.Value)}}}
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
