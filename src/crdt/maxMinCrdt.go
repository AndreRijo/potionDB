//Note: if removes are required, use topKRmv CRDT with a limit of 1
//Also, this can work both as max and minCrdt.

package crdt

import (
	"clocksi"
	"math"
)

const CRDTType_MAXMIN CRDTType = 0

type MaxMinCrdt struct {
	*genericInversibleCRDT
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

func (args MaxAddValue) GetCRDTType() CRDTType { return CRDTType_MAXMIN }

func (args MinAddValue) GetCRDTType() CRDTType { return CRDTType_MAXMIN }

func (args MaxMinState) GetCRDTType() CRDTType { return CRDTType_MAXMIN }

func (args MaxAddValue) MustReplicate() bool { return true }

func (args MinAddValue) MustReplicate() bool { return true }

//Note: crdt can (and most often will be) nil
func (crdt *MaxMinCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int64) (newCrdt CRDT) {
	crdt = &MaxMinCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		topValue:              math.MaxInt64,
	} //TODO: Assign to crdt is potencially unecessary (idea: Updates self in the map (reset operation?))
	newCrdt = crdt
	return
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
	//TODO: Typechecking
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
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		topValue:              crdt.topValue,
	}
	return &newCRDT
}

func (crdt *MaxMinCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth to check if there's a better way of doing this for avg
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
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
