package crdt

import (
	"clocksi"
	"math"
)

type TopKCrdt struct {
	*genericInversibleCRDT
	vc        clocksi.Timestamp
	replicaID int64

	//Max number of elements that can be in top-K
	maxElems int
	//The "smallest score" in the top-K. Useful to know if when a new add arrives it should be added to the top or "hidden" to notInTop
	smallestScore topKElement
	//Elements that are in the top-K
	elems map[int]topKElement
	//Removes of all elements, both in and not in top-K
	rems map[int]clocksi.Timestamp
	//Elems for which either:
	//a) the value is < smallestScore and thus shouldn't be in top
	//b) the value is >= smallestScore yet a higher value for the same ID is already on top, albeit with a smaller TS.
	notInTop map[int]setTopKElement
}

type TopKAdd struct {
	TopKScore
}

type TopKRemove struct {
	Id int
}

type DownstreamTopKAdd struct {
	topKElement
}

type DownstreamTopKRemove struct {
	Id int
	Vc clocksi.Timestamp
}

type TopKScore struct {
	Id    int
	Score int
}

type topKElement struct {
	id        int
	score     int
	ts        int64
	replicaID int64
}

type setTopKElement map[topKElement]struct{}
type setOps map[UpdateArguments]struct{}

//Adds an element to the set. This hides the internal representation of the set
func (set setTopKElement) add(elem topKElement) {
	set[elem] = struct{}{}
}

func (set setOps) add(op UpdateArguments) {
	set[op] = struct{}{}
}

//Returns true if elem is > than other
func (elem topKElement) isHigher(other topKElement) bool {
	if (other == topKElement{}) {
		return true
	}
	if elem.score > other.score {
		return true
	}
	//Same scores. First, compare IDs. If the IDs are also equivalent, then compare timestamps.
	if elem.score == other.score {
		if elem.id > other.id {
			return true
		}
		if elem.id == other.id && elem.ts > other.ts {
			return true
		}
	}
	return false
}

func (elem topKElement) isSmaller(other topKElement) bool {
	return other != topKElement{} && other.isHigher(elem)
}

const (
	defaultTopKSize = 100 //Default number of top positions
)

func (crdt *TopKCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int64) (newCrdt CRDT) {
	return crdt.InitializeWithSize(startTs, replicaID, defaultTopKSize)
}

func (crdt *TopKCrdt) InitializeWithSize(startTs *clocksi.Timestamp, replicaID int64, size int) (newCrdt CRDT) {
	crdt = &TopKCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		vc:                    clocksi.NewClockSiTimestamp(replicaID),
		replicaID:             replicaID,
		maxElems:              size,
		smallestScore:         topKElement{score: math.MinInt32},
		elems:                 make(map[int]topKElement),
		rems:                  make(map[int]clocksi.Timestamp),
		notInTop:              make(map[int]setTopKElement),
	}
	newCrdt = crdt
	return
}

func (crdt *TopKCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	//TODO: Handle updsNotYetApplied

	return
}

func (crdt *TopKCrdt) Update(args UpdateArguments) (downstreamArgs UpdateArguments) {
	//add, rmv: needs to be propagated. add_r, rmv_r - *may* need to be propagated later depending on the operations that are executed in other replicas.
	//or actually, the _r version may be the one that is replicated to the other replicas for fault tolerance? Need to ask that
	//E.g of add_r: consider the operations: add(10, 10, 0), add(10, 5, 5). Now consider that another replica did rem(10) after receiving the first add.
	//In this case, the 2nd add would then be relevant and need to be propagated.

	switch opType := args.(type) {
	case TopKAdd:
		downstreamArgs = crdt.getTopKAddDownstreamArgs(&opType)
	case TopKRemove:
		//Ensuring that the VC of the update and of the CRDT are different instances in order to avoid modifying the upd's accidentally.
		downstreamArgs = crdt.getTopKRemoveDownstreamArgs(&opType)
	}
	return
}

func (crdt *TopKCrdt) getTopKAddDownstreamArgs(addOp *TopKAdd) (args DownstreamTopKAdd) {
	crdt.vc.SelfIncTimestamp(crdt.replicaID)
	elem, hasId := crdt.elems[addOp.Id]
	localArgs := DownstreamTopKAdd{topKElement: topKElement{id: addOp.Id, score: addOp.Score, ts: crdt.vc.GetPos(crdt.replicaID), replicaID: crdt.replicaID}}
	//Check if this element should belong to the top or not
	//An element should belong to the top iff one of those are true:
	//1) Its id is already in the Top but with a smaller score
	//2) Its id is not in the top but it is higher than the smallestScore
	//If an element belongs to the top, then a normal TopKAdd must be returned. Otherwise, a "TopKAdd_r" is returned.
	if hasId && elem.score < addOp.Score {
		//1st
		args = localArgs
	} else if hasId {
		//TODO: add_r?
	} else {
		//2nd, i.e., !hasId
		if elem.isHigher(crdt.smallestScore) {
			args = localArgs
		} else {
			//TODO: add_r?
		}
	}
	return
}

func (crdt *TopKCrdt) getTopKRemoveDownstreamArgs(remOp *TopKRemove) (args DownstreamTopKRemove) {
	//Check if ID is in the topK or in the "notInTop"
	_, inElems := crdt.elems[remOp.Id]
	_, inNotTop := crdt.notInTop[remOp.Id]
	if !inElems && !inNotTop {
		//This remove is irrelevant as this ID isn't in the top and never will be until a more recent add appears
		//TODO: return no-op
	} else if inElems {
		//Remove for element in top-k. Must be propagated
		//Ensuring that the VC of the update and of the CRDT are different instances in order to avoid modifying the upd's accidentally.
		args = DownstreamTopKRemove{Id: remOp.Id, Vc: crdt.vc.Copy()}
	} else {
		//inNotTop = true
		//TODO: rmv_r?
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
		effect = crdt.applyAdd(&opType)
	case DownstreamTopKRemove:
		effect = crdt.applyRemove(&opType)
	}
	return
}

func (crdt *TopKCrdt) applyAdd(op *DownstreamTopKAdd) (effect *Effect) {
	crdt.vc.UpdatePos(op.replicaID, op.ts)
	remsVc, hasEntry := crdt.rems[op.id]
	if !hasEntry || remsVc.GetPos(op.replicaID) < op.ts {
		elem, hasId := crdt.elems[op.id]
		if hasId {
			//Check if the "new elem" is > elem. If it is, add it. On the end, check if min should be updated.
			if op.topKElement.isHigher(elem) {
				crdt.elems[op.id] = op.topKElement
				//Check if min should be updated
				if crdt.smallestScore == elem {
					crdt.findAndUpdateMin()
				}
			} else {
				//Store in notInTop as it might be relevant later on
				crdt.notInTop[op.id].add(op.topKElement)
			}
		} else {
			//Check if it should belong to topK (i.e. there's space or its score is > min)
			if len(crdt.elems) < crdt.maxElems {
				crdt.elems[op.id] = op.topKElement
				//Check if min should be updated
				if (crdt.smallestScore == topKElement{} || crdt.smallestScore.isHigher(op.topKElement)) {
					crdt.smallestScore = op.topKElement
				}
			} else if op.topKElement.isHigher(crdt.smallestScore) {
				//Take min out of top (and store in notInTop) to give space to the new elem with higher score
				crdt.notInTop[crdt.smallestScore.id].add(crdt.smallestScore)
				delete(crdt.elems, crdt.smallestScore.id)
				crdt.findAndUpdateMin()
			}
		}

	} else {
		//Must return this remove to propagate to other replicas
		//TODO: Return remove.
	}
	//TODO: Effects
	return nil
}

func (crdt *TopKCrdt) applyRemove(op *DownstreamTopKRemove) (effect *Effect) {
	rems, hasRems := crdt.rems[op.Id]
	if !hasRems {
		crdt.rems[op.Id] = op.Vc
	} else {
		crdt.rems[op.Id] = rems.Merge(op.Vc)
	}

	//Find all adds for op.Id that are in notInTop whose ts is < clock[replicaID of the rmv] and remove them.
	hiddenForId := crdt.notInTop[op.Id]
	for elem := range hiddenForId {
		if elem.ts < op.Vc.GetPos(elem.replicaID) {
			delete(hiddenForId, elem)
		}
	}
	//Id was totally removed
	if hiddenForId != nil && len(hiddenForId) == 0 {
		delete(crdt.notInTop, op.Id)
	}
	//Also remove from elems if the same condition is true.
	if elem, hasElem := crdt.elems[op.Id]; hasElem && elem.ts < op.Vc.GetPos(elem.replicaID) {
		delete(crdt.elems, op.Id)

		if len(crdt.elems) == crdt.maxElems-1 {
			//Top-k was full previously, need to find the next highest value to replace (possibly in different ID)
			highestElem := topKElement{}
			for id, setElems := range crdt.notInTop {
				if _, has := crdt.elems[id]; !has {
					//We're only interested in IDs which aren't already in the top-k
					for elemNotTop := range setElems {
						if elemNotTop.isHigher(highestElem) {
							highestElem = elemNotTop
						}
					}
				}
			}
			//Check that notInTop actually had elements.
			if (highestElem != topKElement{}) {
				//Promote to top-k and set as min
				crdt.elems[highestElem.id] = highestElem
				crdt.smallestScore = highestElem
			} else if elem == crdt.smallestScore {
				//No elem to add to top-k, however the removed element was the min. Need to find new one
				crdt.findAndUpdateMin()
			}
		} else if elem == crdt.smallestScore {
			//Removed element was the min, need to find new min
			crdt.findAndUpdateMin()
		}

	}

	//TODO: Effects
	return nil
}

//Called whenever the min needs to be updated, due to either an add or a remove
func (crdt *TopKCrdt) findAndUpdateMin() {
	minSoFar := topKElement{}
	for _, elem := range crdt.elems {
		if elem.isSmaller(minSoFar) {
			minSoFar = elem
		}
	}
	crdt.smallestScore = minSoFar
}

func (crdt *TopKCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	//TODO: Typechecking
	return true, nil
}
