package crdt

import (
	"potionDB/src/clocksi"
	"potionDB/src/proto"

	pb "github.com/golang/protobuf/proto"
)

//Note: Implements both CRDT and InversibleCRDT
type CounterCrdt struct {
	*genericInversibleCRDT
	value int32
}

type CounterState struct {
	Value int32
}

type Increment struct {
	Change int32
}

type Decrement struct {
	Change int32
}

type IncrementEffect struct {
	Change int32
}

type DecrementEffect struct {
	Change int32
}

func (crdt *CounterCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_COUNTER }

func (args Increment) GetCRDTType() proto.CRDTType { return proto.CRDTType_COUNTER }

func (args Decrement) GetCRDTType() proto.CRDTType { return proto.CRDTType_COUNTER }

func (args CounterState) GetCRDTType() proto.CRDTType { return proto.CRDTType_COUNTER }

func (args CounterState) GetREADType() proto.READType { return proto.READType_FULL }

func (args Increment) MustReplicate() bool { return true }

func (args Decrement) MustReplicate() bool { return true }

//Note: crdt can (and most often will be) nil
func (crdt *CounterCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &CounterCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		value:                 0,
	}
}

//Used to initialize when building a CRDT from a remote snapshot
func (crdt *CounterCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *CounterCrdt) {
	crdt.genericInversibleCRDT = (&genericInversibleCRDT{}).initialize(startTs)
	return crdt
}

func (crdt *CounterCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return crdt.GetValue()
	}
	counterState := crdt.GetValue().(CounterState)
	for _, upd := range updsNotYetApplied {
		switch typedUpd := (*upd).(type) {
		case Increment:
			counterState.Value += typedUpd.Change
		case Decrement:
			counterState.Value -= typedUpd.Change
		}
	}
	return counterState
}

func (crdt *CounterCrdt) GetValue() (state State) {
	return CounterState{Value: crdt.value}
}

func (crdt *CounterCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	return args.(DownstreamArguments)
}

func (crdt *CounterCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *CounterCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	switch incOrDec := downstreamArgs.(type) {
	case Increment:
		crdt.value += incOrDec.Change
		effectValue = IncrementEffect{Change: incOrDec.Change}
	case Decrement:
		crdt.value -= incOrDec.Change
		effectValue = DecrementEffect{Change: incOrDec.Change}
	}
	return &effectValue
}

func (crdt *CounterCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *CounterCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := CounterCrdt{
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		value:                 crdt.value,
	}
	return &newCRDT
}

func (crdt *CounterCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Optimize and make one specific for counters (no need to redo operations!)
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
}

func (crdt *CounterCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *CounterCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case IncrementEffect:
		crdt.value -= typedEffect.Change
	case DecrementEffect:
		crdt.value += typedEffect.Change
	}
}

func (crdt *CounterCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

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

func (crdt *CounterCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	value := crdt.value
	return &proto.ProtoState{Counter: &proto.ProtoCounterState{Value: &value}}
}

func (crdt *CounterCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	return (&CounterCrdt{value: proto.GetCounter().GetValue()}).initializeFromSnapshot(ts, replicaID)
}
