package crdt

import (
	"fmt"
	rand "math/rand"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	pb "github.com/golang/protobuf/proto"
)

type LwwFlagCrdt struct {
	CRDTVM
	flag           bool
	ts             int64
	replicaID      int16
	localReplicaID int16 //ReplicaID of the replica with this CRDT instance
}

type DownstreamEnableFlagLWW struct {
	Ts        int64
	ReplicaID int16 //replicaID is only used to dinstiguish cases in which Ts is equal
}

type DownstreamDisableFlagLWW struct {
	Ts        int64
	ReplicaID int16
}

// Stores the value previous to the latest setValue
type FlagLWWEffect struct {
	NewFlag   bool
	Ts        int64
	ReplicaID int16
}

func (crdt *LwwFlagCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_LWW }

func (args DownstreamEnableFlagLWW) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_LWW }

func (args DownstreamDisableFlagLWW) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_LWW }

func (args DownstreamEnableFlagLWW) MustReplicate() bool { return true }

func (args DownstreamDisableFlagLWW) MustReplicate() bool { return true }

func (crdt *LwwFlagCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &LwwFlagCrdt{
		CRDTVM:         (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		flag:           false,
		ts:             0,
		replicaID:      replicaID,
		localReplicaID: replicaID,
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *LwwFlagCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *LwwFlagCrdt) {
	crdt.CRDTVM, crdt.localReplicaID = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete), replicaID
	return crdt
}

func (crdt *LwwFlagCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return crdt.GetValue()
	}
	//Correct value is always the one in the last update
	lastUpd := updsNotYetApplied[len(updsNotYetApplied)-1]
	switch lastUpd.(type) {
	case EnableFlag:
		return FlagState{Flag: true}
	case DisableFlag:
		return FlagState{Flag: false}
	}
	return
}

func (crdt *LwwFlagCrdt) GetValue() (state State) {
	return FlagState{Flag: crdt.flag}
}

func (crdt *LwwFlagCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	newTs := time.Now().UTC().UnixNano()
	if newTs < crdt.ts {
		newTs = crdt.ts + rand.Int63n(100)
	}
	switch args.(type) {
	case EnableFlag:
		return DownstreamEnableFlagLWW{Ts: newTs, ReplicaID: crdt.localReplicaID}
	case DisableFlag:
		return DownstreamDisableFlagLWW{Ts: newTs, ReplicaID: crdt.localReplicaID}
	}
	return
}

func (crdt *LwwFlagCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *LwwFlagCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	fmt.Println("[LWWFlag][DS]", downstreamArgs)
	switch typedUpd := downstreamArgs.(type) {
	case DownstreamEnableFlagLWW:
		if typedUpd.Ts > crdt.ts || (typedUpd.Ts == crdt.ts && typedUpd.ReplicaID > crdt.replicaID) {
			effectValue = FlagLWWEffect{NewFlag: crdt.flag, Ts: crdt.ts, ReplicaID: crdt.replicaID} //Store previous values
			crdt.flag, crdt.ts, crdt.replicaID = true, typedUpd.Ts, typedUpd.ReplicaID
		} else {
			effectValue = NoEffect{}
		}
	case DownstreamDisableFlagLWW:
		if typedUpd.Ts > crdt.ts || (typedUpd.Ts == crdt.ts && typedUpd.ReplicaID > crdt.replicaID) {
			effectValue = FlagLWWEffect{NewFlag: crdt.flag, Ts: crdt.ts, ReplicaID: crdt.replicaID} //Store previous values
			crdt.flag, crdt.ts, crdt.replicaID = false, typedUpd.Ts, typedUpd.ReplicaID
		}
	}
	return &effectValue
}

func (crdt *LwwFlagCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *LwwFlagCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := LwwFlagCrdt{
		CRDTVM:         crdt.CRDTVM.copy(),
		flag:           crdt.flag,
		ts:             crdt.ts,
		replicaID:      crdt.replicaID,
		localReplicaID: crdt.localReplicaID,
	}
	return &newCRDT
}

func (crdt *LwwFlagCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth it to make one specific for flags (possibly shared with registers)
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *LwwFlagCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *LwwFlagCrdt) undoEffect(effect *Effect) {
	//Ignore if it is noEffect
	switch typedEffect := (*effect).(type) {
	case FlagLWWEffect:
		crdt.flag, crdt.ts, crdt.replicaID = typedEffect.NewFlag, typedEffect.Ts, typedEffect.ReplicaID
	}
}

func (crdt *LwwFlagCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

// Protobuf functions
func (downOp DownstreamEnableFlagLWW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	flagOp := protobuf.GetFlagOp().GetEnableLWW()
	downOp.Ts, downOp.ReplicaID = flagOp.GetTs(), int16(flagOp.GetReplicaID())
	return downOp
}

func (downOp DownstreamEnableFlagLWW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{FlagOp: &proto.ProtoFlagDownstream{EnableLWW: &proto.ProtoEnableLWWDownstream{
		Ts: pb.Int64(downOp.Ts), ReplicaID: pb.Int32(int32(downOp.ReplicaID)),
	}}}
}

func (downOp DownstreamDisableFlagLWW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	flagOp := protobuf.GetFlagOp().GetDisableLWW()
	downOp.Ts, downOp.ReplicaID = flagOp.GetTs(), int16(flagOp.GetReplicaID())
	return downOp
}

func (downOp DownstreamDisableFlagLWW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{FlagOp: &proto.ProtoFlagDownstream{DisableLWW: &proto.ProtoDisableLWWDownstream{
		Ts: pb.Int64(downOp.Ts), ReplicaID: pb.Int32(int32(downOp.ReplicaID)),
	}}}
}

func (crdt *LwwFlagCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{Flag: &proto.ProtoFlagState{Lww: &proto.ProtoFlagLWWState{
		Flag: pb.Bool(crdt.flag), Ts: pb.Int64(crdt.ts), ReplicaID: pb.Int32(int32(crdt.replicaID)),
	}}}
}

func (crdt *LwwFlagCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	flag := proto.GetFlag().GetLww()
	return (&LwwFlagCrdt{flag: flag.GetFlag(), ts: flag.GetTs(), replicaID: int16(flag.GetReplicaID())}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *LwwFlagCrdt) GetCRDT() CRDT { return crdt }
