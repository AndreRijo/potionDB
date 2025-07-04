package crdt

import (
	"math"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"time"

	pb "google.golang.org/protobuf/proto"
)

// Only supports sets of date, with LWW semantics.
// A much simpler Date CRDT that should fit most common use cases.
type SetOnlyDateCrdt struct {
	CRDTVM
	dateTs         int64
	writeTs        int64 //Timestamp of the last write operation.
	replicaID      int16 //ReplicaID of the last write operation
	localReplicaID int16
}

type DateFullSetOnlyArguments struct{ DateFullArguments }
type DateOnlySetOnlyArguments struct{ DateOnlyArguments }
type TimeSetOnlyArguments struct{ TimeArguments }
type TimestampSetOnlyArguments struct{ TimestampArguments }

type DownstreamSetTsSetOnly struct {
	Value     int64 //The date value being set.
	Ts        int64
	ReplicaID int16 //ReplicaID of the replica that issued the set operation.
}

type SetTsSetOnlyEffect struct { //Values before the operation was applied
	OldValue     int64
	OldTs        int64
	OldReplicaID int16
}

// States and ops are the same from SimpleDateCRDT; queries are embedded from SimpleDateCRDT.

func (crdt *SetOnlyDateCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_SET_ONLY_DATE }

// Downstreams
func (args DownstreamSetTsSetOnly) GetCRDTType() proto.CRDTType { return proto.CRDTType_SET_ONLY_DATE }
func (args DownstreamSetTsSetOnly) MustReplicate() bool         { return true }

func (crdt *SetOnlyDateCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &SetOnlyDateCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		dateTs: GregorianToTs(1, 1, 1), writeTs: math.MinInt64, replicaID: math.MaxInt16, localReplicaID: replicaID,
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *SetOnlyDateCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *SetOnlyDateCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *SetOnlyDateCrdt) IsBigCRDT() bool { return false }

func (crdt *SetOnlyDateCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	var ms int64
	if updsNotYetApplied != nil && len(updsNotYetApplied) > 0 {
		ms = updsNotYetApplied[len(updsNotYetApplied)-1].(DateUpd).ToMS()
	} else {
		ms = crdt.dateTs
	}
	return dateReadHelper(args, ms)
}

func (crdt *SetOnlyDateCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	if dateUpd, ok := args.(DateUpd); ok {
		ms, newTs := dateUpd.ToMS(), time.Now().UTC().UnixNano()
		if newTs < crdt.writeTs { //This may happen as there is a small clock-skew across replicas.
			newTs = crdt.writeTs + (int64(crdt.localReplicaID) % 100) //Generates a new timestamp that is consistent - sequential updates will generate the same ts, which is OK.
		}
		switch args.(type) {
		case SetDate, SetDateOnly, SetDateFull:
			return DownstreamSetTsSetOnly{Value: ms, Ts: newTs, ReplicaID: crdt.localReplicaID}
		case SetTime:
			currMsDay := HourMinSecToMs(ExtractHourMinSec(ms))
			return DownstreamSetTsSetOnly{Value: ms + currMsDay, Ts: newTs, ReplicaID: crdt.localReplicaID}
		}
	}
	return
}

func (crdt *SetOnlyDateCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *SetOnlyDateCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	typedArgs, ok := downstreamArgs.(DownstreamSetTsSetOnly)
	if ok {
		var effectValue Effect
		if typedArgs.Ts > crdt.writeTs || (typedArgs.Ts == crdt.writeTs && typedArgs.ReplicaID <= crdt.replicaID) {
			effectValue = SetTsSetOnlyEffect{OldTs: crdt.dateTs, OldValue: crdt.dateTs, OldReplicaID: crdt.replicaID}
			crdt.dateTs, crdt.writeTs, crdt.replicaID = typedArgs.Value, typedArgs.Ts, typedArgs.ReplicaID
		} else {
			effectValue = NoEffect{}
		}
		return &effectValue
	}
	return
}

func (crdt *SetOnlyDateCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *SetOnlyDateCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := SetOnlyDateCrdt{
		CRDTVM: crdt.CRDTVM.copy(), dateTs: crdt.dateTs, writeTs: crdt.writeTs,
		replicaID: crdt.replicaID, localReplicaID: crdt.localReplicaID,
	}
	return &newCRDT
}

func (crdt *SetOnlyDateCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *SetOnlyDateCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *SetOnlyDateCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case SetTsSetOnlyEffect:
		crdt.dateTs = typedEffect.OldValue
		crdt.writeTs = typedEffect.OldTs
		crdt.replicaID = typedEffect.OldReplicaID
	case NoEffect:
		return
	}
}

func (crdt *SetOnlyDateCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions - most are already defined in simpleDateCrdt. Only need to define downstream and ProtoState.

func (downOp DownstreamSetTsSetOnly) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downProto := protobuf.GetDateOnlyDateOp()
	return DownstreamSetTsSetOnly{Value: downProto.GetValue(), Ts: downProto.GetTs(), ReplicaID: int16(downProto.GetReplicaID())}
}

func (downOp DownstreamSetTsSetOnly) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{DateOnlyDateOp: &proto.ProtoSetOnlyDateDownstream{Value: &downOp.Value, Ts: &downOp.Ts, ReplicaID: pb.Int32(int32(downOp.ReplicaID))}}
}

/*func (crdt *SetOnlyDateCrdt) ToProtoState() (state *proto.ProtoState) {
	return &proto.ProtoState{SetOnlyDate: &proto.ProtoSetOnlyDateState{
		SetClk: crdt.setClk.ToBytes(), SetValue: pb.Int64(crdt.setValue), SetTs: pb.Int64(crdt.setTs),
		SetReplicaID: pb.Int32(int32(crdt.setReplicaID)), Inc: pb.Int64(crdt.inc), CurrClk: crdt.currClk.ToBytes()}}
}

func (crdt *SetOnlyDateCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (sameCRDT *SetWDateCrdt) {
	protoState := proto.GetSetWDate()
	crdt.setClk, crdt.setValue, crdt.setTs = clocksi.ClockSiTimestamp{}.FromBytes(protoState.GetSetClk()), protoState.GetSetValue(), protoState.GetSetTs()
	crdt.setReplicaID, crdt.localReplicaID, crdt.inc = int16(protoState.GetSetReplicaID()), replicaID, protoState.GetInc()
	crdt.currClk = clocksi.ClockSiTimestamp{}.FromBytes(protoState.GetCurrClk())
	return crdt
}*/

func (crdt *SetOnlyDateCrdt) GetCRDT() CRDT { return crdt }

/*
type DownstreamSetTsSetOnly struct {
	Value     int64 //The date value being set.
	Ts        int64
	ReplicaID int16 //ReplicaID of the replica that issued the set operation.
}

type SetTsSetOnlyEffect struct { //Values before the operation was applied
	OldValue     int64
	OldTs        int64
	OldReplicaID int16
}
*/
