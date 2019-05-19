package crdt

import "clocksi"

type TopKCrdt struct {
	*genericInversibleCRDT
	elems     map[string]setTopKElement
	rems      map[string]clocksi.Timestamp
	vc        clocksi.Timestamp
	replicaID int64
}

type TopKAdd struct {
	TopKScore
}

type TopKRemove struct {
	Id string
}

type DownstreamTopKAdd struct {
	TopKScore
	Ts        int64
	ReplicaID int64
}

type DownstreamTopKRemove struct {
	Id string
	Vc clocksi.Timestamp
}

type TopKScore struct {
	Id    string
	Score int
}

type topKElement struct {
	score     int
	ts        int64
	replicaID int64
}

type setTopKElement map[topKElement]struct{}

//Adds an element to the set. This hides the internal representation of the set
func (set setTopKElement) add(elem topKElement) {
	set[elem] = struct{}{}
}

func (crdt *TopKCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int64) (newCrdt CRDT) {
	crdt = &TopKCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		elems:                 make(map[string]setTopKElement),
		rems:                  make(map[string]clocksi.Timestamp),
		vc:                    clocksi.NewClockSiTimestamp(replicaID),
		replicaID:             replicaID,
	}
	newCrdt = crdt
	return
}

func (crdt *TopKCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	//TODO: Handle updsNotYetApplied

	return
}

func (crdt *TopKCrdt) Update(args UpdateArguments) (downstreamArgs UpdateArguments) {
	//TODO: Check gmcabrita's code (lines 102-124) in order to undestand how I should find out here if the downstream should be returned or not.
	//Also I'll prob need to cache UpdateArguments.
	switch opType := args.(type) {
	case TopKAdd:
		//Ensuring that the VC of the update and of the CRDT are different instances in order to avoid modifying the upd's accidentally.
		crdt.vc.SelfIncTimestamp(crdt.replicaID)
		downstreamArgs = DownstreamTopKAdd{TopKScore: opType.TopKScore, Ts: crdt.vc.GetPos(crdt.replicaID), ReplicaID: crdt.replicaID}
	case TopKRemove:
		//Ensuring that the VC of the update and of the CRDT are different instances in order to avoid modifying the upd's accidentally.
		downstreamArgs = DownstreamTopKRemove{Id: opType.Id, Vc: crdt.vc.Copy()}
	}
	return
}

func (crdt *TopKCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs UpdateArguments) {
	crdt.applyDownstream(downstreamArgs)
	//effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	//TODO: Uncomment once effects are implemented
	//crdt.addToHistory(&updTs, &downstreamArgs, effect)
}

func (crdt *TopKCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect *Effect) {
	switch opType := downstreamArgs.(type) {
	case DownstreamTopKAdd:
		effect = crdt.applyAdd(opType)
	case DownstreamTopKRemove:
		effect = crdt.applyRemove(opType)
	}
	return
}

func (crdt *TopKCrdt) applyAdd(op DownstreamTopKAdd) (effect *Effect) {
	remsVc, hasEntry := crdt.rems[op.Id]
	if !hasEntry || remsVc.GetPos(op.ReplicaID) < op.Ts {
		adds, hasAdds := crdt.elems[op.Id]
		if !hasAdds {
			adds = make(setTopKElement)
			crdt.elems[op.Id] = adds
		}
		adds.add(topKElement{score: op.Score, ts: op.Ts, replicaID: op.ReplicaID})
	}
	//TODO: Effects
	return nil
}

func (crdt *TopKCrdt) applyRemove(op DownstreamTopKRemove) (effect *Effect) {
	rems, hasRems := crdt.rems[op.Id]
	if !hasRems {
		crdt.rems[op.Id] = op.Vc
	} else {
		crdt.rems[op.Id] = rems.Merge(op.Vc)
	}

	adds, hasAdds := crdt.elems[op.Id]
	if hasAdds {
		for elem := range adds {
			if elem.ts < op.Vc.GetPos(elem.replicaID) {
				delete(adds, elem)
			}
		}
	}
	//TODO: Effects
	return nil
}

func (crdt *TopKCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	//TODO: Typechecking
	return true, nil
}
