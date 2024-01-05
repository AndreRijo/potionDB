package crdt

import (
	"fmt"
	rand "math/rand"
	"potionDB/src/clocksi"
	"potionDB/src/proto"
	"time"

	pb "github.com/golang/protobuf/proto"
)

//Similar idea to EW Flag: instead of adds generating an unique; removes do.
//Instead of a remove removing all existing uniques, adds do.
type DwFlagCrdt struct {
	CRDTVM
	disables UniqueSet
	//Used to generate unique identifiers. This should not be included in a serialization to transfer the state.
	random rand.Source
}

type DownstreamEnableFlagDW struct {
	Seen UniqueSet
}

type DownstreamDisableFlagDW struct {
	Unique
}

type EnableDWEffect struct {
	Removed UniqueSet //Contains only the uniques that were actually removed
}

type DisableDWEffect struct {
	Unique
}

func (crdt *DwFlagCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_DW }

func (args DownstreamEnableFlagDW) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_DW }

func (args DownstreamDisableFlagDW) GetCRDTType() proto.CRDTType { return proto.CRDTType_FLAG_DW }

func (args DownstreamEnableFlagDW) MustReplicate() bool { return true }

func (args DownstreamDisableFlagDW) MustReplicate() bool { return true }

func (crdt *DwFlagCrdt) Initialize(startTs *clocksi.Timestamp, repliicaID int16) (newCrdt CRDT) {
	return &DwFlagCrdt{
		CRDTVM:   (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		disables: makeUniqueSet(),
		random:   rand.NewSource(time.Now().Unix()),
	}
}

//Used to initialize when building a CRDT from a remote snapshot
func (crdt *DwFlagCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *DwFlagCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *DwFlagCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
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

func (crdt *DwFlagCrdt) GetValue() (state State) {
	fmt.Println("[DWFlag][Read]Returning read:", len(crdt.disables) == 0, (len(crdt.disables)))
	return FlagState{Flag: len(crdt.disables) == 0}
}

func (crdt *DwFlagCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch args.(type) {
	case EnableFlag:
		fmt.Println("[DWFlag][Update]Returning enable upd")
		return DownstreamEnableFlagDW{Seen: crdt.disables.copy()}
	case DisableFlag:
		fmt.Println("[DWFlag][Update]Returning disable upd")
		return DownstreamDisableFlagDW{Unique: Unique(crdt.random.Int63())}
	}
	return
}

func (crdt *DwFlagCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamAgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *DwFlagCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	fmt.Println("[DWFlag][DS]", downstreamArgs)
	switch opType := downstreamArgs.(type) {
	case DownstreamEnableFlagDW:
		return crdt.applyEnableFlag(opType)
	case DownstreamDisableFlagDW:
		return crdt.applyDisableFlag(opType)
	}
	return
}

func (crdt *DwFlagCrdt) applyEnableFlag(op DownstreamEnableFlagDW) (effect *Effect) {
	fmt.Println("[DWFlag][DSEnable]Enabling")
	//For rebuilding older versions, we need to know the actual set of uniques that was effectively removed.
	removed := crdt.disables.getAndRemoveIntersection(op.Seen)
	var effectValue Effect = EnableDWEffect{Removed: removed}
	fmt.Println("[DWFlag]Status after enable: ", crdt.GetValue().(FlagState).Flag, "(len of disables: ", len(crdt.disables), ")")
	return &effectValue
}

func (crdt *DwFlagCrdt) applyDisableFlag(op DownstreamDisableFlagDW) (effect *Effect) {
	fmt.Println("[DWFlag][DSDisable]Disabling")
	//We have causal consistency, so an added unique never comes after its removal.
	crdt.disables.add(op.Unique)
	var effectValue Effect = EnableEWEffect{Unique: op.Unique}
	fmt.Println("[DWFlag]Status after disable: ", crdt.GetValue().(FlagState).Flag, "(len of disables. ", len(crdt.disables), ")")
	return &effectValue
}

func (crdt *DwFlagCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *DwFlagCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := DwFlagCrdt{
		CRDTVM:   crdt.CRDTVM.copy(),
		disables: crdt.disables.copy(),
	}
	return &newCRDT
}

func (crdt *DwFlagCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *DwFlagCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *DwFlagCrdt) undoEffect(effect *Effect) {
	//Ignore if it is noEffect
	switch typedEffect := (*effect).(type) {
	case EnableDWEffect:
		crdt.disables.addAll(typedEffect.Removed)
	case DisableDWEffect:
		delete(crdt.disables, typedEffect.Unique)
	}
}

func (crdt *DwFlagCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions
func (downOp DownstreamEnableFlagDW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Seen = UInt64ArrayToUniqueSet(protobuf.GetFlagOp().GetEnableDW().GetSeenUniques())
	return downOp
}

func (downOp DownstreamEnableFlagDW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{FlagOp: &proto.ProtoFlagDownstream{EnableDW: &proto.ProtoEnableDWDownstream{
		SeenUniques: UniqueSetToUInt64Array(downOp.Seen),
	}}}
}

func (downOp DownstreamDisableFlagDW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Unique = Unique(protobuf.GetFlagOp().GetDisableDW().GetUnique())
	return downOp
}

func (downOp DownstreamDisableFlagDW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{FlagOp: &proto.ProtoFlagDownstream{DisableDW: &proto.ProtoDisableDWDownstream{
		Unique: pb.Uint64(uint64(downOp.Unique)),
	}}}
}

func (crdt *DwFlagCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{Flag: &proto.ProtoFlagState{Dw: &proto.ProtoFlagDWState{Uniques: UniqueSetToUInt64Array(crdt.disables)}}}
}

func (crdt *DwFlagCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	return (&DwFlagCrdt{disables: UInt64ArrayToUniqueSet(proto.GetFlag().GetDw().GetUniques())}).initializeFromSnapshot(ts, replicaID)
}
