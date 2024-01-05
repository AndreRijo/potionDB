package crdt

import (
	"fmt"
	rand "math/rand"
	"potionDB/src/clocksi"
	"potionDB/src/proto"
	"time"

	pb "github.com/golang/protobuf/proto"
)

type EwFlagCrdt struct {
	CRDTVM
	enables UniqueSet
	//Used to generate unique identifiers. This should not be included in a serialization to transfer the state.
	random rand.Source
}

type FlagState struct {
	Flag bool
}

type EnableFlag struct{}

type DisableFlag struct{}

type DownstreamEnableFlagEW struct {
	Unique
}

type DownstreamDisableFlagEW struct {
	Seen UniqueSet
}

type EnableEWEffect struct {
	Unique
}

type DisableEWEffect struct {
	Removed UniqueSet //Contans only the uniques that were actually removed
}

func (crdt *EwFlagCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_EW }

func (args EnableFlag) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_EW }

func (args DisableFlag) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_EW }

func (args DownstreamEnableFlagEW) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_EW }

func (args DownstreamDisableFlagEW) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_EW }

func (args DownstreamEnableFlagEW) MustReplicate() bool { return true }

func (args DownstreamDisableFlagEW) MustReplicate() bool { return true }

func (args FlagState) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_EW }

func (args FlagState) GetREADType() proto.READType { return proto.READType_FULL }

func (crdt *EwFlagCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &EwFlagCrdt{
		CRDTVM:  (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		enables: makeUniqueSet(),
		random:  rand.NewSource(time.Now().Unix()),
	}
}

//Used to initialize when building a CRDT from a remote snapshot
func (crdt *EwFlagCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *EwFlagCrdt) {
	crdt.CRDTVM, crdt.random = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		rand.NewSource(time.Now().Unix())
	return crdt
}

func (crdt *EwFlagCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return crdt.GetValue()
	}
	//Correct value is always the one in the last update
	lastUpd := *updsNotYetApplied[len(updsNotYetApplied)-1]
	switch lastUpd.(type) {
	case EnableFlag:
		return FlagState{Flag: true}
	case DisableFlag:
		return FlagState{Flag: false}
	}
	return
}

func (crdt *EwFlagCrdt) GetValue() (state State) {
	return FlagState{Flag: len(crdt.enables) > 0}
}

func (crdt *EwFlagCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	fmt.Println("[EWFlag[Update]", args)
	switch args.(type) {
	case EnableFlag:
		return DownstreamEnableFlagEW{Unique: Unique(crdt.random.Int63())}
	case DisableFlag:
		return DownstreamDisableFlagEW{Seen: crdt.enables.copy()}
	}
	return
}

func (crdt *EwFlagCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamAgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *EwFlagCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	fmt.Println("[EWFlag][DS]", downstreamArgs)
	switch opType := downstreamArgs.(type) {
	case DownstreamEnableFlagEW:
		return crdt.applyEnableFlag(opType)
	case DownstreamDisableFlagEW:
		return crdt.applyDisableFlag(opType)
	}
	return
}

func (crdt *EwFlagCrdt) applyEnableFlag(op DownstreamEnableFlagEW) (effect *Effect) {
	//We have causal consistency, so an added unique never comes after its removal.
	crdt.enables.add(op.Unique)
	var effectValue Effect = EnableEWEffect{Unique: op.Unique}
	return &effectValue
}

func (crdt *EwFlagCrdt) applyDisableFlag(op DownstreamDisableFlagEW) (effect *Effect) {
	//For rebuilding older versions, we need to know the actual set of uniques that was effectively removed.
	removed := crdt.enables.getAndRemoveIntersection(op.Seen)
	var effectValue Effect = DisableEWEffect{Removed: removed}
	return &effectValue
}

func (crdt *EwFlagCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *EwFlagCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := EwFlagCrdt{
		CRDTVM:  crdt.CRDTVM.copy(),
		enables: crdt.enables.copy(),
	}
	return &newCRDT
}

func (crdt *EwFlagCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *EwFlagCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *EwFlagCrdt) undoEffect(effect *Effect) {
	//Ignore if it is noEffect
	switch typedEffect := (*effect).(type) {
	case EnableEWEffect:
		delete(crdt.enables, typedEffect.Unique)
	case DisableEWEffect:
		crdt.enables.addAll(typedEffect.Removed)
	}
}

func (crdt *EwFlagCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions
func (crdtOp EnableFlag) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return crdtOp
}

func (crdtOp EnableFlag) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Flagop: &proto.ApbFlagUpdate{Value: pb.Bool(true)}}
}

func (crdtOp DisableFlag) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return crdtOp
}

func (crdtOp DisableFlag) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Flagop: &proto.ApbFlagUpdate{Value: pb.Bool(false)}}
}

func (crdtState FlagState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Flag = protobuf.GetFlag().GetValue()
	return crdtState
}

func (crdtState FlagState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Flag: &proto.ApbGetFlagResp{Value: pb.Bool(crdtState.Flag)}}
}

func (downOp DownstreamEnableFlagEW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Unique = Unique(protobuf.GetFlagOp().GetEnableEW().GetUnique())
	return downOp
}

func (downOp DownstreamEnableFlagEW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{FlagOp: &proto.ProtoFlagDownstream{EnableEW: &proto.ProtoEnableEWDownstream{
		Unique: pb.Uint64(uint64(downOp.Unique)),
	}}}
}

func (downOp DownstreamDisableFlagEW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Seen = UInt64ArrayToUniqueSet(protobuf.GetFlagOp().GetDisableEW().GetSeenUniques())
	return downOp
}

func (downOp DownstreamDisableFlagEW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{FlagOp: &proto.ProtoFlagDownstream{DisableEW: &proto.ProtoDisableEWDownstream{
		SeenUniques: UniqueSetToUInt64Array(downOp.Seen),
	}}}
}

func (crdt *EwFlagCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{Flag: &proto.ProtoFlagState{Ew: &proto.ProtoFlagEWState{Uniques: UniqueSetToUInt64Array(crdt.enables)}}}
}

func (crdt *EwFlagCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	return (&EwFlagCrdt{enables: UInt64ArrayToUniqueSet(proto.GetFlag().GetEw().GetUniques())}).initializeFromSnapshot(ts, replicaID)
}
