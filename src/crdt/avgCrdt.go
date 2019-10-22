package crdt

import (
	"clocksi"
	"proto"
)

type AvgCrdt struct {
	*genericInversibleCRDT
	sum   int64
	nAdds int64
}

type AvgState struct {
	Value float64
}

type AddValue struct {
	Value int64
}

type AddMultipleValue struct {
	SumValue int64
	NAdds    int64
}

type AddMultipleValueEffect AddMultipleValue

func (args AddValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AddMultipleValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AvgState) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AvgState) GetREADType() proto.READType { return proto.READType_FULL }

func (args AddMultipleValue) MustReplicate() bool { return true }

//Note: crdt can (and most often will be) nil
func (crdt *AvgCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	crdt = &AvgCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		sum:                   0,
		nAdds:                 0,
	} //TODO: Assign to crdt is potencially unecessary (idea: Updates self in the map (reset operation?))
	newCrdt = crdt
	return
}

func (crdt *AvgCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) > 0 {
		return crdt.GetValue()
	}
	//var sum int64 = crdt.sum
	//var nAdds int64 = crdt.nAdds
	sum, nAdds := crdt.sum, crdt.nAdds
	for _, upd := range updsNotYetApplied {
		typedUpd := (*upd).(AddMultipleValue)
		sum += typedUpd.SumValue
		nAdds += typedUpd.NAdds
	}
	return AvgState{Value: float64(sum) / float64(nAdds)}
}

func (crdt *AvgCrdt) GetValue() (state State) {
	return AvgState{Value: float64(crdt.sum) / float64(crdt.nAdds)}
}

func (crdt *AvgCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch typedArgs := args.(type) {
	case AddValue:
		downstreamArgs = AddMultipleValue{SumValue: typedArgs.Value, NAdds: 1}
	case AddMultipleValue:
		downstreamArgs = args.(DownstreamArguments)
	}
	return
}

func (crdt *AvgCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *AvgCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	typedUpd := downstreamArgs.(AddMultipleValue)
	var effectValue Effect = AddMultipleValueEffect(typedUpd)
	crdt.sum += typedUpd.SumValue
	crdt.nAdds += typedUpd.NAdds
	return &effectValue
}

func (crdt *AvgCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	//TODO: Typechecking
	return true, nil
}

func (crdt *AvgCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := AvgCrdt{
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		sum:                   crdt.sum,
		nAdds:                 crdt.nAdds,
	}
	return &newCRDT
}

func (crdt *AvgCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth to check if there's a better way of doing this for avg
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
}

func (crdt *AvgCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *AvgCrdt) undoEffect(effect *Effect) {
	typedEffect := (*effect).(AddMultipleValueEffect)
	crdt.sum -= typedEffect.SumValue
	crdt.nAdds -= typedEffect.NAdds
}

func (crdt *AvgCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}
