package crdt

import (
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
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
