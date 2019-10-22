package crdt

/*
import (
	"clocksi"
	"proto"
)

const ()

//Very similar to RWEmbMapCRDT. Check the comments there, as the algorithm is pretty much the same

type RWEmbArrayCrdt struct {
	*genericInversibleCRDT
	entries   []CRDT
	removes   []map[int16]int64 //replicaID -> clk value
	replicaID int16             //Needed for some embedded CRDTs and for rmvClock
}

//States returned by queries

type EmbArrayState struct {
	States []State
}

type EmbGetPosState struct {
	State State
}

//Queries

type EmbArrayGetPosArguments struct {
	Pos int
}

//Operations

type EmbArrayUpdate struct {
	Pos int
	Upd UpdateArguments
}

type EmbArrayUpdateAll struct {
	Upds map[int]UpdateArguments
}

type EmbArrayRemove struct {
	Pos int
}

type EmbArrayRemoveAll struct {
	PosToRemove []int
}

//Downstream operations

type DownstreamRWEmbArrayUpdate struct {
	Pos        int
	Upd        DownstreamArguments
	RmvEntries map[int16]int64
	ReplicaID  int16
}

type DownstreamRWEmbArrayUpdateAll struct {
	Upds       map[int]DownstreamArguments
	RmvEntries map[int16]int64
	ReplicaID  int16
}

type DownstreamRWEmbArrayRemoveAll struct {
	Rems      []int
	ReplicaID int16
	Ts        int64
}

//Operations effects for inversibleCRDT
//TODO

//Upds

func (args EmbArrayUpdate) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

func (args EmbArrayUpdateAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

func (args EmbArrayRemove) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

//Downstreams

func (args DownstreamRWEmbArrayUpdate) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

func (args DownstreamRWEmbArrayUpdate) MustReplicate() bool { return true }

func (args DownstreamRWEmbArrayUpdateAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

func (args DownstreamRWEmbArrayUpdateAll) MustReplicate() bool { return true }

func (args DownstreamRWEmbArrayRemoveAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

func (args DownstreamRWEmbArrayRemoveAll) MustReplicate() bool { return true }

//States

func (args EmbArrayState) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

func (args EmbArrayState) GetREADType() proto.READType { return proto.READType_FULL }

func (args EmbGetPosState) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

func (args EmbGetPosState) GetREADType() proto.READType { return proto.READType_GET_POS }

//Queries

func (args EmbMapGetValueArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_RRARRAY }

func (args EmbMapGetValueArguments) GetREADType() proto.READType { return proto.READType_GET_POS }

func (crdt *RWEmbArrayCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	crdt = new & RWEmbArrayCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		entries:               nil,
		removes:               nil,
		replicaID:             replicaID,
	}
	return
}

func (crdt *RWEmbArrayCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	//TODO: updsNotYetApplied
	switch typedArg := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case EmbMapGetValueArguments:
		return crdt.getValue(updsNotYetApplied)
	}
	return nil
}

func (crdt *RWEmbArrayCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch opType := args.(type) {
	case EmbArrayUpdate:
		downstreamArgs = crdt.getUpdateArgs(opType.Pos, opType.Upd)
	case EmbArrayUpdateAll:
		downstreamArgs = crdt.getUpdateAllArgs(opType.Upds)
	case EmbArrayRemove:
		downstreamArgs = crdt.getRemoveArgs([]int{opType.Pos})
	case EmbArrayRemoveAll:
		downstreamArgs = crdt.getRemoveAllArgs(opType.PosToRemove)
	}
	return
}
func (crdt *RWEmbArrayCrdt) getUpdateArgs(pos int, upd UpdateArguments) (downstreamArgs DownstreamRWEmbArrayUpdate) {
	embCrdt, _ := crdt.getOrCreateEmbCrdt(pos, upd)
	embUp := embCrdt.Update(upd)
	if (embUpd != NoOp{} && embUpd.MustReplicate()) {
		rmvEntries := make(map[int64]int64)
		if len(crdt.removes) > pos {
			rmvClk := crdt.removes[pos]
			if rmvClk != nil {
				for replica, clk := range rmvClk {
					rmvEntries[replica] = clk
				}
			}
		}
		return DownstreamRWEmbArrayUpdate{Pos: int, Upd: embUp, RmvEntries: rmvEntries}
	}
	return NoOp{}
}

func (crdt *RWEmbArrayCrdt) getUpdateAllArgs(upds map[int]UpdateArguments) (downstreamArgs DownstreamRWEmbArrayUpdateAll) {
	downstreams := make(map[int]DownstreamArguments)
	rmvEntries := make(map[int64]int64)
	for pos, upd := range upds {
		embCrdt, _ := crdt.getOrCreateEmbCrdt(pos, upd)
		embUp := embCrdt.Update(upd)
		if (embUpd != NoOp{} && embUpd.MustReplicate()) {
			downstreams[pos] = embUp
		}
		if len(crdt.removes) > pos {
			rmvClk := crdt.removes[pos]
			if rmvClk != nil {
				for replica, clk := range rmvClk {
					existingClk := rmvEntries[replica]
					if clk > existingClk {
						rmvEntries[replica] = clk
					}
				}
			}
		}
	}

	if len(downstreams) == 0 {
		return NoOp{}
	}
	return DownstreamRWEmbArrayUpdateAll{Upds: downstreams, RmvEntries: rmvEntries, ReplicaID: crdt.replicaID}
}

func (crdt *RWEmbArrayCrdt) getRemoveAllArgs(posToRemove []int) (downstreamArgs DownstreamRWEmbArrayRemoveAll) {
	toRemove := make([]int, len(posToRemove))
	i := 0
	for _, pos := range posToRemove {
		if len(crdt.entries) > pos && crdt.entries[pos] != nil {
			toRemove[i] = pos
			i++
		}
	}
	if i == 0 {
		return NoOp{}
	}
	return DownstreamRWEmbArrayRemoveAll{Rems: toRemove[:i], ReplicaID: crdt.replicaID, Ts: crdt.rmvClock.GetPos(crdt.replicaID + 1)}
}

func (crdt *RWEmbArrayCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	//TODO: effects
	_, otherDownstreamArgs = crdt.applyDownstream(updTs, downstreamArgs)
	return
}

func (crdt *RWEmbArrayCrdt) applyDownstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (effect *Effect,
	otherDownstreamArgs DownstreamArguments) {
	var tmpEffect Effect = NoEffect{}
	switch opType := downstreamArgs.(type) {
	case DownstreamRWEmbArrayUpdate:
		tmpEffect, otherDownstreamArgs = crdt.applyUpdate(updTs, opType.Pos, opType.Upd, opType.RmvEntries, opType.ReplicaID)
	case DownstreamRWEmbArrayUpdateAll:
		tmpEffect, otherDownstreamArgs = crdt.applyUpdateAll(updTs, opType.Upds, opType.RmvEntries, opType.ReplicaID)
	case DownstreamRWEmbArrayRemoveAll:
		tmpEffect = crdt.applyRemoveAll()
	}
	return &tmpEffect, otherDownstreamArgs
}

func (crdt *RWEmbArrayCrdt) applyUpdate(updTs clocksi.Timestamp, pos int, upd DownstreamArguments,
	remClks map[int64]int64, remoteID int64) (effect *Effect, otherDownstreamArgs DownstreamArguments) {

	canAdd = len(crdt.entries) < pos
	if !canAdd {
		//Check if there's a remove
		remEntry := crdt.removes[pos]
		canAdd = true
		if remEntry != nil {
			//All remValues must be >= than ts
			for replica, ts := range remEntry {
				if remValue, has := remClks[replica]; !has || ts > remValue {
					canAdd = false
					break
				}
			}
		}
	}

	if canAdd {
		embCRDT, new := crdt.getOrCreateEmbCrdt(pos, upd)
		embDownstream := embCRDT.Downstream(updTs, upd)
		if new {
			if len(crdt.entries) < pos {
				newArray := make([]CRDT, pos, pos)
				copy(newArray, crdt.entries)
				crdt.entries = newArray
				newRems := make([]map[int64]int64, pos, pos)
				copy(newRems, crdt.removes)
				crdt.removes = newRems
			}
			crdt.entries[pos] = embCRDT
		}
		if embDownstream != nil {
			return nil, DownstreamRWEmbArrayUpdate{Upd: embDownstream, RmvEntries: remClks}
		}
	}

	return nil, nil
}

func (crdt *RWEmbArrayCrdt) applyUpdateAll(updTs clocksi.Timestamp, upds map[int]DownstreamArguments,
	remClks map[int64]int64, remoteID int64) (effect *Effect, otherDownstreamArgs DownstreamArguments) {

	newDown := make(map[int]DownstreamArguments)

	for key, upd := range upds {
		//TODO: Code repetition with applyUpdate

	}
}

func (crdt *RWEmbArrayCrdt) applyRemoveAll(toRem []int, remoteID int64, remoteClk int64) (effect *Effect) {

}

func (crdt *RWEmbArrayCrdt) Copy() (copyCRDT InversibleCRDT) {

	return crdt
}

func (crdt *RWEmbArrayCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {

}

func (crdt *RWEmbArrayCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return nil
}

func (crdt *RWEmbArrayCrdt) undoEffect(effect *Effect) {

}

func (crdt *RWEmbArrayCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {

}

//Others

func (crdt *RWEmbArrayCrdt) getOrCreateEmbCrdt(pos int, upd UpdateArguments) (embCrdt CRDT, new bool) {
	if pos > len(crdt.entries) {
		return InitializeCrdt(upd.GetCRDTType(), crdt.replicaID), true
	}
	embCrdt = crdt.entries[pos]
	if embCrdt == nil {
		return InitializeCrdt(upd.GetCRDTType(), crdt.replicaID), true
	}
	return embCrdt, false
}
*/
