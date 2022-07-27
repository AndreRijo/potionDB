//Note: if removes are required, use topKRmv CRDT with a limit of 1
//Also, this can work both as max and minCrdt.

package crdt

import (
	"math"
	"potionDB/src/clocksi"
	"potionDB/src/proto"

	pb "github.com/golang/protobuf/proto"
)

type MaxMinCrdt struct {
	CRDTVM
	topValue int64
}

type MaxMinState struct {
	Value int64
}

type MaxAddValue struct {
	Value int64
}

type MinAddValue struct {
	Value int64
}

type MaxMinAddValueEffect struct {
	PreviousValue int64
}

const (
	IS_MAX = true
	IS_MIN = false
)

func (crdt *MaxMinCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAXMIN }

func (args MaxAddValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAXMIN }

func (args MinAddValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAXMIN }

func (args MaxMinState) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAXMIN }

func (args MaxMinState) GetREADType() proto.READType { return proto.READType_FULL }

func (args MaxAddValue) MustReplicate() bool { return true }

func (args MinAddValue) MustReplicate() bool { return true }

//Note: crdt can (and most often will be) nil
func (crdt *MaxMinCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &MaxMinCrdt{
		CRDTVM:   (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		topValue: math.MaxInt64,
	}
}

//Used to initialize when building a CRDT from a remote snapshot
func (crdt *MaxMinCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *MaxMinCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *MaxMinCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) > 0 {
		return crdt.GetValue()
	}
	result := crdt.GetValue().(MaxMinState)
	for _, upd := range updsNotYetApplied {
		switch typedUpd := (*upd).(type) {
		case MaxAddValue:
			result.Value = crdt.max(result.Value, typedUpd.Value)
		case MinAddValue:
			result.Value = crdt.min(result.Value, typedUpd.Value)
		}
	}
	return result
}

func (crdt *MaxMinCrdt) GetValue() (state State) {
	return MaxMinState{Value: crdt.topValue}
}

func (crdt *MaxMinCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	if crdt.topValue == math.MaxInt64 {
		return args.(DownstreamArguments)
	}
	//The update might be irrelevant (e.g: MaxAddValue whose value is < crdt.TopValue)
	//This optimization will need to be removed if we support removes later on
	switch typedUpd := args.(type) {
	case MaxAddValue:
		if typedUpd.Value < crdt.topValue {
			return NoOp{}
		}
	case MinAddValue:
		if typedUpd.Value > crdt.topValue {
			return NoOp{}
		}
	}
	return args.(DownstreamArguments)
}

func (crdt *MaxMinCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *MaxMinCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	previousValue := crdt.topValue
	switch typedUpd := downstreamArgs.(type) {
	case MaxAddValue:
		crdt.topValue = crdt.max(crdt.topValue, typedUpd.Value)
		if previousValue == crdt.topValue {
			effectValue = NoEffect{}
		} else {
			effectValue = MaxMinAddValueEffect{PreviousValue: previousValue}
		}
	case MinAddValue:
		crdt.topValue = crdt.min(crdt.topValue, typedUpd.Value)
		if previousValue == crdt.topValue {
			effectValue = NoEffect{}
		} else {
			effectValue = MaxMinAddValueEffect{PreviousValue: previousValue}
		}
	}
	return &effectValue
}

func (crdt *MaxMinCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *MaxMinCrdt) max(first, second int64) (max int64) {
	if first == math.MaxInt64 || second > first {
		return second
	}
	return first
}

func (crdt *MaxMinCrdt) min(first, second int64) (min int64) {
	if first == math.MaxInt64 || second < first {
		return second
	}
	return first
}

func (crdt *MaxMinCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := MaxMinCrdt{
		CRDTVM:   crdt.CRDTVM.copy(),
		topValue: crdt.topValue,
	}
	return &newCRDT
}

func (crdt *MaxMinCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth to check if there's a better way of doing this for avg
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *MaxMinCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *MaxMinCrdt) undoEffect(effect *Effect) {
	typedEffect := (*effect).(MaxMinAddValueEffect)
	if crdt.topValue != typedEffect.PreviousValue {
		//Value changed due to this update, so we revert to the previous value
		crdt.topValue = typedEffect.PreviousValue
	}
}

func (crdt *MaxMinCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

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

func (crdt *MaxMinCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	topValue := crdt.topValue
	return &proto.ProtoState{Maxmin: &proto.ProtoMaxMinState{Value: &topValue}}
}

func (crdt *MaxMinCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	return (&MaxMinCrdt{topValue: proto.GetMaxmin().GetValue()}).initializeFromSnapshot(ts, replicaID)
}
