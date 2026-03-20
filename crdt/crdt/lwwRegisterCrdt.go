package crdt

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

// Note: Implements both CRDT and InversibleCRDT
type LwwRegisterCrdt struct {
	CRDTVM
	value any
	/*ts             int64
	replicaID      int16
	localReplicaID int16 //ReplicaID of the replica with this CRDT instance*/
	tsId tsWithReplicaID //(ts << 10) | replicaID. The highest 54 bits correspond to the lowest 54 bits of a 64-bit timestamp; the lowest 10 bits are used for replicaID.
}

type tsWithReplicaID uint64

func makeTsWithReplicaID(ts int64, replicaID uint16) tsWithReplicaID {
	return tsWithReplicaID((uint64(ts) << shared.BITS_FOR_REPLICA_ID) | uint64(replicaID))
}

func (t tsWithReplicaID) getTs() int64 {
	return int64(t >> shared.BITS_FOR_REPLICA_ID)
}

func (t tsWithReplicaID) getReplicaID() uint16 {
	return uint16(t & ((1 << shared.BITS_FOR_REPLICA_ID) - 1))
}

type RegisterState struct {
	Value any
}

type SetValue struct {
	NewValue any
}

type DownstreamSetValue struct {
	NewValue any
	//Ts        int64
	//ReplicaID int16 //replicaID is only used to dinstiguish cases in which Ts is equal
	TsId tsWithReplicaID
}

// Stores the value previous to the latest setValue
type SetValueEffect struct {
	NewValue any
	//Ts        int64
	//ReplicaID int16
	TsId tsWithReplicaID
}

func (crdt *LwwRegisterCrdt) GetCRDTType() proto.CRDTType   { return proto.CRDTType_LWWREG }
func (crdt *LwwRegisterCrdt) GetDATAType() proto.DATAType   { return proto.DATAType_DEFAULT }
func (args SetValue) GetCRDTType() proto.CRDTType           { return proto.CRDTType_LWWREG }
func (args SetValue) GetDATAType() proto.DATAType           { return proto.DATAType_DEFAULT }
func (args DownstreamSetValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_LWWREG }
func (args DownstreamSetValue) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state RegisterState) GetCRDTType() proto.CRDTType     { return proto.CRDTType_LWWREG }
func (state RegisterState) GetDATAType() proto.DATAType     { return proto.DATAType_DEFAULT }
func (state RegisterState) GetREADType() proto.READType     { return proto.READType_FULL }

func (args DownstreamSetValue) MustReplicate() bool { return true }

// Utility function. Not necessary for this CRDT but may be useful when manipulating its read state.
func (args RegisterState) ToFloat64() (value float64) {
	valueI, err := strconv.ParseInt(args.Value.(string), 10, 64)
	if err != nil {
		valueF, errF := strconv.ParseFloat(args.Value.(string), 64)
		if errF != nil {
			fmt.Println("[LWWRegister]Error parsing RegisterState value in aggregateStates. Both float and int conversions failed.")
			fmt.Println("[LWWRegister]ErrInt:", err, "ErrFloat:", errF, "Register value:", args.Value)
			panic(1)
		}
		value = valueF
	} else {
		value = float64(valueI)
	}
	return
}

// Note: crdt can (and most often will be) nil
func (crdt *LwwRegisterCrdt) Initialize(startTs *clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	//crdt = &LwwRegisterCrdt{value: "", ts: 0, replicaID: replicaID, localReplicaID: replicaID}
	crdt = &LwwRegisterCrdt{value: "", tsId: 0}
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *LwwRegisterCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID uint16) (sameCRDT *LwwRegisterCrdt) {
	//crdt.CRDTVM, crdt.localReplicaID = (&genericInversibleCRDT{}).initialize(crdt), replicaID
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *LwwRegisterCrdt) IsBigCRDT() bool { return false }

func (crdt *LwwRegisterCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	if len(updsNotYetApplied) == 0 {
		return crdt.GetValue()
	}
	//Correct value is always the one in the last update
	return RegisterState{Value: (updsNotYetApplied[len(updsNotYetApplied)-1]).(SetValue).NewValue}
}

func (crdt *LwwRegisterCrdt) GetValue() (state State) {
	return RegisterState{Value: crdt.value}
}

// Note: Does not support MultiUpd, as this CRDT is core to efficiency (also, what would be the point of multiUpds here...?)
func (crdt *LwwRegisterCrdt) Update(args UpdateArguments) (downStreamArgs DownstreamArguments) {
	/*newTs := time.Now().UnixNano()
	if newTs < crdt.ts { //This may happen as there is a small clock-skew across replicas.
		newTs = crdt.ts + (int64(crdt.localReplicaID) % 100) //Generates a new timestamp that is consistent - sequential updates will generate the same ts, which is OK.
		//newTs = crdt.ts + rand.Int63n(100)
	}*/
	newTs := makeTsWithReplicaID(time.Now().UnixNano(), shared.ReplicaID)
	if newTs < crdt.tsId { //This may happen as there is a small clock-skew across replicas.
		//Generates a new timestamp that is consistent - sequential updates will generate the same ts, which is OK.
		//newTs = (crdt.tsId && 0xFFFFFFFFFFFF0000) + (((uint64(shared.ReplicaID)) % 100) << 16) + uint64(shared.ReplicaID)
		newTs = makeTsWithReplicaID(crdt.tsId.getTs()+int64(shared.ReplicaID)%100, shared.ReplicaID)
	}
	return DownstreamSetValue{NewValue: args.(SetValue).NewValue, TsId: newTs}
	//return DownstreamSetValue{NewValue: args.(SetValue).NewValue, ReplicaID: crdt.localReplicaID, Ts: newTs}
}

func (crdt *LwwRegisterCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *LwwRegisterCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	setValue := downstreamArgs.(DownstreamSetValue)
	var effectValue Effect
	/*if setValue.Ts > crdt.ts || (setValue.Ts == crdt.ts && setValue.ReplicaID >= crdt.replicaID) {
		effectValue = SetValueEffect{Ts: crdt.ts, NewValue: crdt.value, ReplicaID: crdt.replicaID}
		crdt.ts, crdt.replicaID, crdt.value = setValue.Ts, setValue.ReplicaID, setValue.NewValue
	} */
	if setValue.TsId > crdt.tsId { //This already handles replicaID in case of equal ts.
		effectValue = SetValueEffect{TsId: crdt.tsId, NewValue: crdt.value}
		crdt.tsId, crdt.value = setValue.TsId, setValue.NewValue
	} else {
		effectValue = NoEffect{}
	}
	return &effectValue
}

func (crdt *LwwRegisterCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *LwwRegisterCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := LwwRegisterCrdt{
		CRDTVM: crdt.CRDTVM.copy(),
		value:  crdt.value,
		tsId:   crdt.tsId,
		/*ts:             crdt.ts,
		replicaID:      crdt.replicaID,
		localReplicaID: crdt.localReplicaID,*/
	}
	return &newCRDT
}

func (crdt *LwwRegisterCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth it to make one specific for registers
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *LwwRegisterCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *LwwRegisterCrdt) undoEffect(effect *Effect) {
	//Ignore if it is noEffect
	switch typedEffect := (*effect).(type) {
	case SetValueEffect:
		//crdt.value, crdt.ts, crdt.replicaID = typedEffect.NewValue, typedEffect.Ts, typedEffect.ReplicaID
		crdt.value, crdt.tsId = typedEffect.NewValue, typedEffect.TsId
	}
}

func (crdt *LwwRegisterCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp SetValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	//crdtOp.NewValue = string(protobuf.GetRegop().GetValue())
	bytesValue := protobuf.GetRegop().GetValue()
	crdtOp.NewValue = unsafe.String(&bytesValue[0], len(bytesValue))
	return crdtOp
}

func (crdtOp SetValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	if stringV, ok := crdtOp.NewValue.(string); ok {
		return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Regop{Regop: &proto.ApbRegUpdate{Value: unsafe.Slice(unsafe.StringData(stringV), len(stringV))}}}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Regop{Regop: &proto.ApbRegUpdate{Value: crdtOp.NewValue.([]byte)}}}
	//return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Regop{Regop: &proto.ApbRegUpdate{Value: []byte(crdtOp.NewValue.(string))}}}
}

func (crdtState RegisterState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Value = string(protobuf.GetReg().GetValue())
	return crdtState
}

func (crdtState RegisterState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Reg{Reg: &proto.ApbGetRegResp{Value: []byte((crdtState.Value).(string))}}}
}

func (downOp DownstreamSetValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	regOp := protobuf.GetLwwregOp()
	value := regOp.GetValue()
	downOp.NewValue, downOp.TsId = unsafe.String(&value[0], len(value)), tsWithReplicaID(regOp.GetTsId())
	//downOp.NewValue, downOp.TsId = string(regOp.GetValue()), tsWithReplicaID(regOp.GetTsId())
	//downOp.NewValue, downOp.ReplicaID, downOp.Ts = string(regOp.GetValue()), int16(regOp.GetReplicaID()), regOp.GetTs()
	return downOp
}

func (downOp DownstreamSetValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	/*return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_LwwregOp{LwwregOp: &proto.ProtoLWWRegisterDownstream{
		Value: []byte(downOp.NewValue.(string)), Ts: pb.Int64(downOp.Ts), ReplicaID: pb.Int32(int32(downOp.ReplicaID)),
	}}*/
	if stringV, ok := downOp.NewValue.(string); ok {
		return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_LwwregOp{LwwregOp: &proto.ProtoLWWRegisterDownstream{Value: unsafe.Slice(unsafe.StringData(stringV), len(stringV)), TsId: pb.Uint64(uint64(downOp.TsId))}}}
		//return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_LwwregOp{LwwregOp: &proto.ProtoLWWRegisterDownstream{Value: []byte(stringV), TsId: pb.Uint64(uint64(downOp.TsId))}}}
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_LwwregOp{LwwregOp: &proto.ProtoLWWRegisterDownstream{Value: downOp.NewValue.([]byte), TsId: pb.Uint64(uint64(downOp.TsId))}}}
}

func (crdt *LwwRegisterCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	/*value, ts, replicaID := crdt.value, crdt.ts, int32(crdt.replicaID)
	return &proto.ProtoState{Lwwreg: &proto.ProtoLWWRegState{Value: []byte((value).(string)), Ts: &ts, ReplicaID: &replicaID}}*/
	return &proto.ProtoState{State: &proto.ProtoState_Lwwreg{Lwwreg: &proto.ProtoLWWRegState{Value: []byte((crdt.value).(string)), TsId: pb.Uint64(uint64(crdt.tsId))}}}
}

func (crdt *LwwRegisterCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID uint16) (newCRDT CRDT) {
	lwwRegProto := proto.GetLwwreg()
	/*return (&LwwRegisterCrdt{value: string(lwwRegProto.GetValue()), ts: lwwRegProto.GetTs(),
	replicaID: int16(lwwRegProto.GetReplicaID()), localReplicaID: crdt.replicaID}).initializeFromSnapshot(ts, replicaID)*/
	return (&LwwRegisterCrdt{value: string(lwwRegProto.GetValue()), tsId: tsWithReplicaID(lwwRegProto.GetTsId())}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *LwwRegisterCrdt) GetCRDT() CRDT { return crdt }
