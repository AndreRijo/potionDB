package crdt

import (
	"fmt"
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
	writeTs        int64  //Timestamp of the last write operation.
	replicaID      uint16 //ReplicaID of the last write operation
	localReplicaID uint16
}

type DateFullSetOnlyArguments struct{ DateFullArguments }
type DateOnlySetOnlyArguments struct{ DateOnlyArguments }
type TimeSetOnlyArguments struct{ TimeArguments }
type TimestampSetOnlyArguments struct{ TimestampArguments }

// Updates supported:
// SetDateFull, SetDate, SetDateOnly, SetTime, SetMSSetOnly
type SetMSSetOnly int64

type DownstreamSetTsSetOnly struct {
	Value     int64 //The date value being set.
	Ts        int64
	ReplicaID uint16 //ReplicaID of the replica that issued the set operation.
}

type SetTsSetOnlyEffect struct { //Values before the operation was applied
	OldValue     int64
	OldTs        int64
	OldReplicaID uint16
}

// States and ops are the same from SimpleDateCRDT; queries are embedded from SimpleDateCRDT.

func (crdt *SetOnlyDateCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_SET_ONLY_DATE }
func (crdt *SetOnlyDateCrdt) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

// Ops
func (args SetMSSetOnly) GetCRDTType() proto.CRDTType { return proto.CRDTType_SET_ONLY_DATE }
func (args SetMSSetOnly) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

// Downstreams
func (args DownstreamSetTsSetOnly) GetCRDTType() proto.CRDTType { return proto.CRDTType_SET_ONLY_DATE }
func (args DownstreamSetTsSetOnly) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args DownstreamSetTsSetOnly) MustReplicate() bool         { return true }

func (crdt *SetOnlyDateCrdt) Initialize(startTs clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	return &SetOnlyDateCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(crdt),
		dateTs: GregorianToTs(1, 1, 1), writeTs: math.MinInt64, replicaID: math.MaxInt16, localReplicaID: replicaID,
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *SetOnlyDateCrdt) initializeFromSnapshot(startTs clocksi.Timestamp, replicaID uint16) (sameCRDT *SetOnlyDateCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *SetOnlyDateCrdt) IsBigCRDT() bool { return false }

func (crdt *SetOnlyDateCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	var ms int64
	if len(updsNotYetApplied) > 0 {
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
	} else if multiUpd, ok := args.(MultiUpd); ok {
		multiDowns := make(MultiUpd, len(multiUpd))
		for i, innerUpd := range multiUpd {
			multiDowns[i] = crdt.Update(innerUpd)
		}
		return multiDowns
	} else {
		fmt.Printf("[SetOnlyDateCrdt][Update]Unknown update type: %v (%T)\n", args, args)
	}
	return
}

func (crdt *SetOnlyDateCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		for _, upd := range multiUpd {
			crdt.Downstream(updTs, upd.(DownstreamArguments))
		}
		return nil
	}
	crdt.addToHistory(updTs, downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *SetOnlyDateCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect Effect) {
	typedArgs, ok := downstreamArgs.(DownstreamSetTsSetOnly)
	if ok {
		if typedArgs.Ts > crdt.writeTs || (typedArgs.Ts == crdt.writeTs && typedArgs.ReplicaID <= crdt.replicaID) {
			effect = SetTsSetOnlyEffect{OldTs: crdt.dateTs, OldValue: crdt.dateTs, OldReplicaID: crdt.replicaID}
			crdt.dateTs, crdt.writeTs, crdt.replicaID = typedArgs.Value, typedArgs.Ts, typedArgs.ReplicaID
		} else {
			effect = NoEffect{}
		}
	} else {
		effect = NoEffect{}
		fmt.Printf("[SetOnlyDateCrdt][Downstream]Unsupported downstream type: %v (%T)\n", downstreamArgs, downstreamArgs)
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

func (crdt *SetOnlyDateCrdt) reapplyOp(updArgs DownstreamArguments) (effect Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *SetOnlyDateCrdt) undoEffect(effect Effect) {
	switch typedEffect := (effect).(type) {
	case SetTsSetOnlyEffect:
		crdt.dateTs = typedEffect.OldValue
		crdt.writeTs = typedEffect.OldTs
		crdt.replicaID = typedEffect.OldReplicaID
	case NoEffect:
		return
	}
}

func (crdt *SetOnlyDateCrdt) notifyRebuiltComplete(currTs clocksi.Timestamp) {}

//Protobuf functions - most are already defined in simpleDateCrdt. Only need to define downstream, ProtoState and SetMS.

func (crdtOp SetMSSetOnly) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return SetMSSetOnly(protobuf.GetDateop().GetSetMS().GetMs())
}

func (crdtOp SetMSSetOnly) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Dateop{Dateop: &proto.ApbDateUpdate{Upd: &proto.ApbDateUpdate_SetMS{SetMS: &proto.ApbSetMS{Ms: pb.Int64(int64(crdtOp))}}}}}
}

func (downOp DownstreamSetTsSetOnly) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downProto := protobuf.GetSetOnlyDateOp()
	return DownstreamSetTsSetOnly{Value: downProto.GetValue(), Ts: downProto.GetTs(), ReplicaID: uint16(downProto.GetReplicaID())}
}

func (downOp DownstreamSetTsSetOnly) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_SetOnlyDateOp{SetOnlyDateOp: &proto.ProtoSetOnlyDateDownstream{Value: &downOp.Value, Ts: &downOp.Ts, ReplicaID: pb.Int32(int32(downOp.ReplicaID))}}}
}

func (crdt *SetOnlyDateCrdt) ToProtoState() (state *proto.ProtoState) {
	return &proto.ProtoState{State: &proto.ProtoState_SetOnlyDate{SetOnlyDate: &proto.ProtoSetOnlyDateState{
		DateTs: pb.Int64(crdt.dateTs), WriteTs: pb.Int64(crdt.writeTs), ReplicaID: pb.Int32(int32(crdt.replicaID)),
	}}}
}

func (crdt *SetOnlyDateCrdt) FromProtoState(proto *proto.ProtoState, ts clocksi.Timestamp, replicaID uint16) (sameCRDT *SetOnlyDateCrdt) {
	protoState := proto.GetSetOnlyDate()
	crdt.dateTs, crdt.writeTs, crdt.replicaID = protoState.GetDateTs(), protoState.GetWriteTs(), uint16(protoState.GetReplicaID())
	return crdt
}

func (crdt *SetOnlyDateCrdt) GetCRDT() CRDT { return crdt }

/*
type DownstreamSetTsSetOnly struct {
	Value     int64 //The date value being set.
	Ts        int64
	ReplicaID uint16 //ReplicaID of the replica that issued the set operation.
}

type SetTsSetOnlyEffect struct { //Values before the operation was applied
	OldValue     int64
	OldTs        int64
	OldReplicaID uint16
}
*/
