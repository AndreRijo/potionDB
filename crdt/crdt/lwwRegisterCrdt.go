package crdt

import (
	"fmt"
	rand "math/rand"
	"strconv"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

// Note: Implements both CRDT and InversibleCRDT
type LwwRegisterCrdt struct {
	CRDTVM
	value          interface{}
	ts             int64
	replicaID      int16
	localReplicaID int16 //ReplicaID of the replica with this CRDT instance
}

type RegisterState struct {
	Value interface{}
}

type SetValue struct {
	NewValue interface{}
}

type DownstreamSetValue struct {
	NewValue  interface{}
	Ts        int64
	ReplicaID int16 //replicaID is only used to dinstiguish cases in which Ts is equal
}

// Stores the value previous to the latest setValue
type SetValueEffect struct {
	NewValue  interface{}
	Ts        int64
	ReplicaID int16
}

func (crdt *LwwRegisterCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_LWWREG }

func (args SetValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_LWWREG }

func (args DownstreamSetValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_LWWREG }

func (args RegisterState) GetCRDTType() proto.CRDTType { return proto.CRDTType_LWWREG }

func (args RegisterState) GetREADType() proto.READType { return proto.READType_FULL }

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
func (crdt *LwwRegisterCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &LwwRegisterCrdt{
		CRDTVM:         (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		value:          "",
		ts:             0,
		replicaID:      replicaID,
		localReplicaID: replicaID,
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *LwwRegisterCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *LwwRegisterCrdt) {
	crdt.CRDTVM, crdt.localReplicaID = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete), replicaID
	return crdt
}

func (crdt *LwwRegisterCrdt) IsBigCRDT() bool { return false }

func (crdt *LwwRegisterCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return crdt.GetValue()
	}
	//Correct value is always the one in the last update
	return RegisterState{Value: (updsNotYetApplied[len(updsNotYetApplied)-1]).(SetValue).NewValue}
}

func (crdt *LwwRegisterCrdt) GetValue() (state State) {
	return RegisterState{Value: crdt.value}
}

func (crdt *LwwRegisterCrdt) Update(args UpdateArguments) (downStreamArgs DownstreamArguments) {
	newTs := time.Now().UTC().UnixNano()
	if newTs < crdt.ts {
		newTs = crdt.ts + rand.Int63n(100)
	}
	return DownstreamSetValue{NewValue: args.(SetValue).NewValue, ReplicaID: crdt.localReplicaID, Ts: newTs}
}

func (crdt *LwwRegisterCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *LwwRegisterCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	setValue := downstreamArgs.(DownstreamSetValue)
	var effectValue Effect
	if setValue.Ts > crdt.ts || (setValue.Ts == crdt.ts && setValue.ReplicaID > crdt.replicaID) {
		effectValue = SetValueEffect{Ts: crdt.ts, NewValue: crdt.value, ReplicaID: crdt.replicaID}
		crdt.ts, crdt.replicaID, crdt.value = setValue.Ts, setValue.ReplicaID, setValue.NewValue
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
		CRDTVM:         crdt.CRDTVM.copy(),
		value:          crdt.value,
		ts:             crdt.ts,
		replicaID:      crdt.replicaID,
		localReplicaID: crdt.localReplicaID,
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
		crdt.value, crdt.ts, crdt.replicaID = typedEffect.NewValue, typedEffect.Ts, typedEffect.ReplicaID
	}
}

func (crdt *LwwRegisterCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

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
	downOp.NewValue, downOp.ReplicaID, downOp.Ts = string(regOp.GetValue()), int16(regOp.GetReplicaID()), regOp.GetTs()
	return downOp
}

func (downOp DownstreamSetValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{LwwregOp: &proto.ProtoLWWRegisterDownstream{
		Value: []byte(downOp.NewValue.(string)), Ts: pb.Int64(downOp.Ts), ReplicaID: pb.Int32(int32(downOp.ReplicaID)),
	}}
}

func (crdt *LwwRegisterCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	value, ts, replicaID := crdt.value, crdt.ts, int32(crdt.replicaID)
	return &proto.ProtoState{Lwwreg: &proto.ProtoLWWRegState{Value: []byte((value).(string)), Ts: &ts, ReplicaID: &replicaID}}
}

func (crdt *LwwRegisterCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	lwwRegProto := proto.GetLwwreg()
	return (&LwwRegisterCrdt{value: string(lwwRegProto.GetValue()), ts: lwwRegProto.GetTs(),
		replicaID: int16(lwwRegProto.GetReplicaID()), localReplicaID: crdt.replicaID}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *LwwRegisterCrdt) GetCRDT() CRDT { return crdt }
