package crdt

import (
	"clocksi"
	rand "math/rand"
	"time"
)

const CRDTType_LWWREG CRDTType = 5

//Note: Implements both CRDT and InversibleCRDT
type LwwRegisterCrdt struct {
	*genericInversibleCRDT
	value          interface{}
	ts             int64
	replicaID      int64
	localReplicaID int64 //ReplicaID of the replica with this CRDT instance
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
	ReplicaID int64 //replicaID is only used to dinstiguish cases in which Ts is equal
}

//Stores the value previous to the latest setValue
type SetValueEffect struct {
	NewValue  interface{}
	Ts        int64
	ReplicaID int64
}

func (args SetValue) GetCRDTType() CRDTType { return CRDTType_LWWREG }

func (args DownstreamSetValue) GetCRDTType() CRDTType { return CRDTType_LWWREG }

func (args RegisterState) GetCRDTType() CRDTType { return CRDTType_LWWREG }

func (args DownstreamSetValue) MustReplicate() bool { return true }

//Note: crdt can (and most often will be) nil
func (crdt *LwwRegisterCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int64) (newCrdt CRDT) {
	crdt = &LwwRegisterCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		value:                 "",
		ts:                    0,
		replicaID:             replicaID,
		localReplicaID:        replicaID,
	} //TODO: Assign to crdt is potencially unecessary (idea: Updates self in the map (reset operation?))
	newCrdt = crdt
	return
}

func (crdt *LwwRegisterCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) > 0 {
		return crdt.GetValue()
	}
	//Correct value is always the one in the last update
	return RegisterState{Value: updsNotYetApplied[len(updsNotYetApplied)-1].(SetValue).NewValue}
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
	//TODO: Typechecking
	return true, nil
}

func (crdt *LwwRegisterCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := LwwRegisterCrdt{
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		value:                 crdt.value,
		ts:                    crdt.ts,
		replicaID:             crdt.replicaID,
	}
	return &newCRDT
}

func (crdt *LwwRegisterCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth it to make one specific for registers
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
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
