package crdt

import (
	"clocksi"
	"fmt"
	"math"
	"proto"
	"sort"

	pb "github.com/golang/protobuf/proto"
)

type TopKRmvCrdt struct {
	*genericInversibleCRDT
	vc        clocksi.Timestamp
	replicaID int16

	//Max number of elements that can be in top-K
	maxElems int
	//The "smallest score" in the top-K. Useful to know if when a new add arrives it should be added to the top or "hidden" to notInTop
	smallestScore TopKElement
	//Elements that are in the top-K
	elems map[int32]TopKElement
	//Removes of all elements, both in and not in top-K
	rems map[int32]clocksi.Timestamp
	//Elems for which either:
	//a) the value is < smallestScore and thus shouldn't be in top
	//b) the value is >= smallestScore yet a higher value for the same ID is already on top, albeit with a smaller TS.
	notInTop map[int32]setTopKElement
}

type TopKValueState struct {
	Scores []TopKScore
}

type TopKAdd struct {
	TopKScore
}

type TopKRemove struct {
	Id int32
}

type DownstreamTopKAdd struct {
	TopKElement
}

type DownstreamTopKRemove struct {
	Id int32
	Vc clocksi.Timestamp
}

//A topkadd that doesn't necessarily need to be replicated for other than fault-tolerance purposes
type OptDownstreamTopKAdd struct {
	DownstreamTopKAdd
}

//A topkrmv that doesn't necessarily need to be replicated for other than fault-tolerance purposes
type OptDownstreamTopKRemove struct {
	DownstreamTopKRemove
}

//Queries
type GetTopNArguments struct {
	NumberEntries int32
}

type GetTopKAboveValueArguments struct {
	MinValue int32
}

type TopKScore struct {
	Id    int32
	Score int32
}

type TopKElement struct {
	Id        int32
	Score     int32
	Ts        int64
	ReplicaID int16
}

//Effect of an TopKAdd that adds the element to the top
type TopKAddEffect struct {
	TopKElement
	oldTs int64 //Used to make the crdt's VC go "back in time"
}

//Effect of an TopKAdd that adds the element to notInTop
type TopKAddNotTopEffect struct {
	TopKAddEffect
}

//Effect of an TopKAdd which replaced an older, lower value with a higher one, possibly changing the min.
type TopKReplaceEffect struct {
	newElem TopKElement
	oldElem TopKElement
	oldMin  TopKElement
	oldTs   int64 //Used to make the crdt's VC go "back in time"
}

type TopKRemoveEffect struct {
	id            int32
	previousVc    clocksi.Timestamp //nil in case there wasn't a previous remove for this Id
	notTopRemoved setTopKElement
	remElem       TopKElement
	oldMin        TopKElement
	minAddedToTop bool //true if oldMin was updated to the top
}

type setTopKElement map[TopKElement]struct{}
type setOps map[UpdateArguments]struct{}

//Adds an element to the set. This hides the internal representation of the set
func (set setTopKElement) add(elem TopKElement) {
	set[elem] = struct{}{}
}

func (set setTopKElement) copy() (newSet setTopKElement) {
	newSet = make(setTopKElement)
	for id, elem := range set {
		newSet[id] = elem
	}
	return
}

func (set setOps) add(op UpdateArguments) {
	set[op] = struct{}{}
}

//Returns true if elem is > than other
func (elem TopKElement) isHigher(other TopKElement) bool {
	if (other == TopKElement{}) {
		return true
	}
	if elem.Score > other.Score {
		return true
	}
	//Same scores. First, compare IDs. If the IDs are also equivalent, then compare timestamps.
	if elem.Score == other.Score {
		if elem.Id > other.Id {
			return true
		}
		if elem.Id == other.Id && elem.Ts > other.Ts {
			return true
		}
	}
	return false
}

func (elem TopKElement) isSmaller(other TopKElement) bool {
	return other != TopKElement{} && other.isHigher(elem)
}

//Ops
func (args TopKAdd) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (args TopKRemove) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

//Downstreams
func (args DownstreamTopKAdd) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (args DownstreamTopKRemove) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (args OptDownstreamTopKAdd) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (args OptDownstreamTopKRemove) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (args DownstreamTopKAdd) MustReplicate() bool { return true }

func (args DownstreamTopKRemove) MustReplicate() bool { return true }

func (args OptDownstreamTopKAdd) MustReplicate() bool { return false }

func (args OptDownstreamTopKRemove) MustReplicate() bool { return false }

//States
func (args TopKValueState) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (args TopKValueState) GetREADType() proto.READType { return proto.READType_FULL }

//Reads
func (args GetTopNArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (args GetTopNArguments) GetREADType() proto.READType { return proto.READType_GET_N }

func (args GetTopKAboveValueArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (args GetTopKAboveValueArguments) GetREADType() proto.READType {
	return proto.READType_GET_ABOVE_VALUE
}

const (
	defaultTopKSize = 100 //Default number of top positions
)

func (crdt *TopKRmvCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return crdt.InitializeWithSize(startTs, replicaID, defaultTopKSize)
}

func (crdt *TopKRmvCrdt) InitializeWithSize(startTs *clocksi.Timestamp, replicaID int16, size int) (newCrdt CRDT) {
	crdt = &TopKRmvCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		vc:                    clocksi.NewClockSiTimestamp(replicaID),
		replicaID:             replicaID,
		maxElems:              size,
		smallestScore:         TopKElement{Score: math.MinInt32},
		elems:                 make(map[int32]TopKElement),
		rems:                  make(map[int32]clocksi.Timestamp),
		notInTop:              make(map[int32]setTopKElement),
	}
	newCrdt = crdt
	return
}

func (crdt *TopKRmvCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	//TODO: Consider updsNotYetApplied in all of these
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case GetTopNArguments:
		return crdt.getTopN(typedArgs.NumberEntries, updsNotYetApplied)
	case GetTopKAboveValueArguments:
		return crdt.getTopKAboveValue(typedArgs.MinValue, updsNotYetApplied)
	}
	return nil
}

func (crdt *TopKRmvCrdt) getState(updsNotYetApplied []*UpdateArguments) (state State) {
	values := make([]TopKScore, len(crdt.elems))
	i := 0
	for _, elem := range crdt.elems {
		values[i] = TopKScore{Id: elem.Id, Score: elem.Score}
		i++
	}
	return TopKValueState{Scores: values}
}

func (crdt *TopKRmvCrdt) getTopN(numberEntries int32, updsNotYetApplied []*UpdateArguments) (state State) {
	//TODO: Think of a way to make this more optimized
	allValues := crdt.getState(updsNotYetApplied).(TopKValueState).Scores
	sort.Slice(allValues, func(i, j int) bool { return allValues[i].Score > allValues[j].Score })
	if numberEntries >= int32(len(allValues)) {
		return TopKValueState{Scores: allValues}
	}

	//Include extra values until allValues[curr] < allValues[numberEntries - 1]
	min := allValues[numberEntries-1]
	//Include extra values until allValues[numberEntries - 1] < min
	for ; numberEntries < int32(len(allValues)) && allValues[numberEntries] == min; numberEntries++ {

	}
	return TopKValueState{Scores: allValues[:numberEntries]}
}

func (crdt *TopKRmvCrdt) getTopKAboveValue(minValue int32, updsNotYetApplied []*UpdateArguments) (state State) {
	values := make([]TopKScore, len(crdt.elems))
	actuallyAdded := 0
	for _, elem := range crdt.elems {
		if elem.Score >= minValue {
			values[actuallyAdded] = TopKScore{Id: elem.Id, Score: elem.Score}
			actuallyAdded++
		}
	}
	return TopKValueState{Scores: values[:actuallyAdded]}
}

func (crdt *TopKRmvCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
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

func (crdt *TopKRmvCrdt) getTopKAddDownstreamArgs(addOp *TopKAdd) (args DownstreamArguments) {
	crdt.vc.SelfIncTimestamp(crdt.replicaID)
	elem, hasId := crdt.elems[addOp.Id]
	localArgs := DownstreamTopKAdd{TopKElement: TopKElement{Id: addOp.Id, Score: addOp.Score, Ts: crdt.vc.GetPos(crdt.replicaID), ReplicaID: crdt.replicaID}}
	//Check if this element should belong to the top or not
	//An element should belong to the top iff one of those are true:
	//1) Its id is already in the Top but with a smaller score
	//2) Its id is not in the top but it is higher than the smallestScore
	//If an element belongs to the top, then a normal TopKAdd must be returned. Otherwise, a "TopKAdd_r" is returned.
	if hasId && elem.Score < addOp.Score {
		//1st
		args = localArgs
	} else if hasId {
		//add_r
		args = OptDownstreamTopKAdd{DownstreamTopKAdd: localArgs}
		//args = localArgs
	} else {
		//2nd, i.e., !hasId
		if elem.isHigher(crdt.smallestScore) {
			args = localArgs
		} else {
			//add_r
			args = OptDownstreamTopKAdd{DownstreamTopKAdd: localArgs}
			//args = localArgs
		}
	}
	return
}

func (crdt *TopKRmvCrdt) getTopKRemoveDownstreamArgs(remOp *TopKRemove) (args DownstreamArguments) {
	//Check if ID is in the topK or in the "notInTop"
	_, inElems := crdt.elems[remOp.Id]
	_, inNotTop := crdt.notInTop[remOp.Id]
	if !inElems && !inNotTop {
		//This remove is irrelevant as this ID isn't in the top and never will be until a more recent add appears
		args = NoOp{}
	} else if inElems {
		//Remove for element in top-k. Must be propagated
		//Ensuring that the VC of the update and of the CRDT are different instances in order to avoid modifying the upd's accidentally.
		args = DownstreamTopKRemove{Id: remOp.Id, Vc: crdt.vc.Copy()}
	} else {
		//inNotTop = true
		//rmv_r
		args = OptDownstreamTopKRemove{DownstreamTopKRemove: DownstreamTopKRemove{Id: remOp.Id, Vc: crdt.vc.Copy()}}
		//args = DownstreamTopKRemove{Id: remOp.Id, Vc: crdt.vc.Copy()}
	}
	return
}

func (crdt *TopKRmvCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect, otherDownstreamArgs := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)
	return
}

func (crdt *TopKRmvCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	switch opType := downstreamArgs.(type) {
	case DownstreamTopKAdd:
		effect, otherDownstreamArgs = crdt.applyAdd(&opType)
	case DownstreamTopKRemove:
		effect, otherDownstreamArgs = crdt.applyRemove(&opType)
	//Local replicas must still apply these
	case OptDownstreamTopKAdd:
		effect, otherDownstreamArgs = crdt.applyAdd(&opType.DownstreamTopKAdd)
	case OptDownstreamTopKRemove:
		effect, otherDownstreamArgs = crdt.applyRemove(&opType.DownstreamTopKRemove)
	}
	return
}

//Effect addToTop (include previousMin and previousEntry, if any)?
//Effect addToNotTop
func (crdt *TopKRmvCrdt) applyAdd(op *DownstreamTopKAdd) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	//fmt.Println("Applying topK add")
	var effectValue Effect
	oldTs := crdt.vc.GetPos(op.ReplicaID)

	crdt.vc.UpdatePos(op.ReplicaID, op.Ts)
	remsVc, hasEntry := crdt.rems[op.Id]
	if !hasEntry || remsVc.GetPos(op.ReplicaID) < op.Ts {
		elem, hasId := crdt.elems[op.Id]
		if hasId {
			//Check if the "new elem" is > elem. If it is, add it. On the end, check if min should be updated.
			if op.TopKElement.isHigher(elem) {
				effectValue = TopKReplaceEffect{newElem: op.TopKElement, oldElem: elem, oldMin: crdt.smallestScore, oldTs: oldTs}
				crdt.elems[op.Id] = op.TopKElement
				//Different replicas, so we'll keep the old entry in notInTop (as it might be relevant again depending on concurrent removes)
				if elem.ReplicaID != op.TopKElement.ReplicaID {
					entry, hasEntry := crdt.notInTop[op.Id]
					if !hasEntry {
						entry = make(setTopKElement)
						crdt.notInTop[op.Id] = entry
					}
					entry.add(elem)
				}
				//Check if min should be updated
				if crdt.smallestScore == elem {
					crdt.findAndUpdateMin()
				}
			} else {
				effectValue = TopKAddNotTopEffect{TopKAddEffect: TopKAddEffect{TopKElement: op.TopKElement, oldTs: oldTs}}
				//Store in notInTop as it might be relevant later on
				entry, hasEntry := crdt.notInTop[op.Id]
				if !hasEntry {
					entry = make(setTopKElement)
					crdt.notInTop[op.Id] = entry
				}
				entry.add(op.TopKElement)
			}
		} else {
			//Check if it should belong to topK (i.e. there's space or its score is > min)
			if len(crdt.elems) < crdt.maxElems {
				crdt.elems[op.Id] = op.TopKElement
				//Check if min should be updated
				if (crdt.smallestScore == TopKElement{} || crdt.smallestScore.isHigher(op.TopKElement)) {
					effectValue = TopKReplaceEffect{newElem: op.TopKElement, oldMin: crdt.smallestScore, oldTs: oldTs}
					crdt.smallestScore = op.TopKElement
				} else {
					effectValue = TopKAddEffect{TopKElement: op.TopKElement, oldTs: oldTs}
				}
			} else if op.TopKElement.isHigher(crdt.smallestScore) {
				effectValue = TopKReplaceEffect{newElem: op.TopKElement, oldElem: crdt.smallestScore, oldMin: crdt.smallestScore, oldTs: oldTs}
				//Take min out of top (and store in notInTop) to give space to the new elem with higher score
				//Note that it's possible to have existing entries for this ID in notInTop. The opposite is also possible.
				entry, hasEntry := crdt.notInTop[crdt.smallestScore.Id]
				if !hasEntry {
					entry = make(setTopKElement)
					crdt.notInTop[crdt.smallestScore.Id] = entry
				}
				entry.add(crdt.smallestScore)
				delete(crdt.elems, crdt.smallestScore.Id)
				crdt.elems[op.Id] = op.TopKElement
				crdt.findAndUpdateMin()
			} else {
				effectValue = TopKAddNotTopEffect{TopKAddEffect: TopKAddEffect{TopKElement: op.TopKElement, oldTs: oldTs}}
				//effectValue = TopKAddEffect{TopKElement: op.TopKElement}
				//Add to notInTop
				entry, hasEntry := crdt.notInTop[op.Id]
				if !hasEntry {
					entry = make(setTopKElement)
					crdt.notInTop[op.Id] = entry
				}
				entry.add(op.TopKElement)
			}
		}

	} else {
		//Must return this remove to propagate to other replicas
		otherDownstreamArgs = DownstreamTopKRemove{Id: op.Id, Vc: remsVc}
		effectValue = NoEffect{}
		fmt.Println("[TOPKRMV]Apply add is returning a new remove.")
	}
	return &effectValue, otherDownstreamArgs
}

/*
All remove effects are going to need:
- Previous VC (or nil, if there wasn't any)
- entries retired from notInTop
- need to know if the element was removed from top...?
Variants for element removed:
- Nothing happened;
- Only min was updated;
- TopK was full previously:
	- Was able to find an element to add, which is now the min;
	- Wasn't able to find an element to add and the removed one was the min;
	- Didn't find an element to add;
*/
func (crdt *TopKRmvCrdt) applyRemove(op *DownstreamTopKRemove) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	//Must be <= as the remove's clk is a copy without incrementing.
	remEffect := TopKRemoveEffect{id: op.Id, notTopRemoved: make(setTopKElement)}
	rems, hasRems := crdt.rems[op.Id]
	if !hasRems {
		remEffect.previousVc = nil
		crdt.rems[op.Id] = op.Vc
	} else {
		remEffect.previousVc = rems.Copy()
		crdt.rems[op.Id] = rems.Merge(op.Vc)
	}

	//Find all adds for op.Id that are in notInTop whose ts is < clock[replicaID of the rmv] and remove them.
	hiddenForId := crdt.notInTop[op.Id]
	for elem := range hiddenForId {
		if elem.Ts <= op.Vc.GetPos(elem.ReplicaID) {
			delete(hiddenForId, elem)
			remEffect.notTopRemoved.add(elem)
		}
	}
	//Id was totally removed
	if hiddenForId != nil && len(hiddenForId) == 0 {
		delete(crdt.notInTop, op.Id)
	}
	//Also remove from elems if the same condition is true.
	if elem, hasElem := crdt.elems[op.Id]; hasElem && elem.Ts <= op.Vc.GetPos(elem.ReplicaID) {
		delete(crdt.elems, op.Id)
		remEffect.remElem = elem
		remEffect.oldMin = crdt.smallestScore

		if len(crdt.elems) == crdt.maxElems-1 {
			//Top-k was full previously, need to find the next highest value to replace (possibly in different ID)
			highestElem := TopKElement{}
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
			if (highestElem != TopKElement{}) {
				//Promote to top-k and set as min
				crdt.elems[highestElem.Id] = highestElem
				crdt.smallestScore = highestElem
				otherDownstreamArgs = DownstreamTopKAdd{TopKElement: highestElem}
				//Also remove it from notInTop
				notTop := crdt.notInTop[highestElem.Id]
				delete(notTop, highestElem)
				if len(notTop) == 0 {
					delete(crdt.notInTop, highestElem.Id)
				}

				remEffect.minAddedToTop = true
			} else if elem == crdt.smallestScore {
				//No elem to add to top-k, however the removed element was the min. Need to find new one
				crdt.findAndUpdateMin()
			}
		} else if hiddenForId != nil && len(hiddenForId) > 0 {
			//There's no other element in notInTop that isn't already in top but, for the element we removed,
			//there's other entries still left. The highest should go to top.
			highest := TopKElement{}
			for elemNotTop := range hiddenForId {
				if elemNotTop.isHigher(highest) {
					highest = elemNotTop
				}
			}
			delete(hiddenForId, highest)
			if len(hiddenForId) == 0 {
				delete(crdt.notInTop, highest.Id)
			}
			crdt.elems[highest.Id] = highest
			if elem == crdt.smallestScore {
				//If the removed elem was the min, the one we added now has, for sure, an even smaller value
				crdt.smallestScore = highest
			}
			//Need to pass this add now
			//TODO: Analyze if it really needs to be downstreamed or not
			otherDownstreamArgs = DownstreamTopKAdd{TopKElement: highest}
		} else if elem == crdt.smallestScore {
			//Removed element was the min, need to find new min
			crdt.findAndUpdateMin()
		}

	}

	var eff Effect = remEffect
	return &eff, otherDownstreamArgs
}

//Called whenever the min needs to be updated, due to either an add or a remove
func (crdt *TopKRmvCrdt) findAndUpdateMin() {
	minSoFar := TopKElement{}
	for _, elem := range crdt.elems {
		if elem.isSmaller(minSoFar) {
			minSoFar = elem
		}
	}
	crdt.smallestScore = minSoFar
}

func (crdt *TopKRmvCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

//METHODS FOR INVERSIBLE CRDT

func (crdt *TopKRmvCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := TopKRmvCrdt{
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		vc:                    crdt.vc.Copy(),
		replicaID:             crdt.replicaID,
		maxElems:              crdt.maxElems,
		smallestScore:         crdt.smallestScore,
		elems:                 make(map[int32]TopKElement),
		rems:                  make(map[int32]clocksi.Timestamp),
		notInTop:              make(map[int32]setTopKElement),
	}

	for id, elem := range crdt.elems {
		newCrdt.elems[id] = elem
	}
	for id, clk := range crdt.rems {
		newCrdt.rems[id] = clk.Copy()
	}
	for id, set := range crdt.notInTop {
		newCrdt.notInTop[id] = set.copy()
	}

	return &newCrdt
}

func (crdt *TopKRmvCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
}

func (crdt *TopKRmvCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	effect, _ = crdt.applyDownstream(updArgs)
	return
}

func (crdt *TopKRmvCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case TopKAddEffect:
		crdt.undoAddEffect(&typedEffect)
	case TopKAddNotTopEffect:
		crdt.undoAddNotTopEffect(&typedEffect)
	case TopKReplaceEffect:
		crdt.undoReplaceEffect(&typedEffect)
	case TopKRemoveEffect:
		crdt.undoRemoveEffect(&typedEffect)
	}
}

func (crdt *TopKRmvCrdt) undoAddEffect(effect *TopKAddEffect) {
	crdt.vc.UpdateForcedPos(effect.ReplicaID, effect.oldTs)
	//Element was added to top and there wasn't any entry for it previously on top
	delete(crdt.elems, effect.Id)
	//Both effects will need the old Ts.
	/*
		Possible cases:
		- Add is there but with higher value. In this case, it only removes entry from notInTop
		- Add is there with this value. In this case we would need to search for the higher value (would be better to store it in the effect). And possibly it might disapear from top...
		- Add isn't there but it is in notInTop. Remove it from there and nothing else changes
		I also need the old ts...
	*/
}

func (crdt *TopKRmvCrdt) undoAddNotTopEffect(effect *TopKAddNotTopEffect) {
	crdt.vc.UpdateForcedPos(effect.ReplicaID, effect.oldTs)
	//Element was added to notInTop
	notInTop := crdt.notInTop[effect.Id]
	delete(notInTop, effect.TopKElement)
	if len(notInTop) == 0 {
		delete(crdt.notInTop, effect.Id)
	}
}

func (crdt *TopKRmvCrdt) undoReplaceEffect(effect *TopKReplaceEffect) {
	crdt.vc.UpdateForcedPos(effect.newElem.ReplicaID, effect.oldTs)
	//New element was added to the top
	delete(crdt.elems, effect.newElem.Id)
	//If there was an old element, it could had been in the top or notInTop. Regardless, it now belongs to the top
	if (effect.oldElem != TopKElement{}) {
		crdt.elems[effect.oldElem.Id] = effect.oldElem
		//If the old element was in notInTop, remove it from there
		if notInTop, has := crdt.notInTop[effect.oldElem.Id]; has {
			delete(notInTop, effect.oldElem)
			if len(notInTop) == 0 {
				delete(crdt.notInTop, effect.oldElem.Id)
			}
		}
	}
	//Update the min
	crdt.smallestScore = effect.oldMin
}

/*
All remove effects are going to need:
- Previous VC (or nil, if there wasn't any)
- entries retired from notInTop
- need to know if the element was removed from top...?
Variants for element removed:
- Nothing happened;
- Only min was updated;
- TopK was full previously:
	- Was able to find an element to add, which is now the min;
	- Wasn't able to find an element to add and the removed one was the min;
	- Didn't find an element to add;
*/
func (crdt *TopKRmvCrdt) undoRemoveEffect(effect *TopKRemoveEffect) {
	if effect.previousVc == nil {
		//There was no remove previous to this one
		delete(crdt.rems, effect.id)
	} else {
		crdt.rems[effect.id] = effect.previousVc.Copy()
	}

	notTop, has := crdt.notInTop[effect.id]
	if !has {
		notTop = make(setTopKElement)
		crdt.notInTop[effect.id] = notTop
	}
	//Add elements removed from notInTop
	for elem := range effect.notTopRemoved {
		notTop.add(elem)
	}

	if (effect.remElem != TopKElement{}) {
		//An element was removed.
		//When this remove was executed, it's possible that a concurrent add in notInTop for the same elem was promoted to the top
		inTop, has := crdt.elems[effect.remElem.Id]
		if has {
			//Move that element to notInTop
			notTop, has = crdt.notInTop[effect.id]
			if !has {
				notTop = make(setTopKElement)
				crdt.notInTop[effect.id] = notTop
			}
			notTop.add(inTop)
		}
		//Add removed element back to the top
		crdt.elems[effect.id] = effect.remElem
		if effect.minAddedToTop {
			//When the remove was executed, the actual min was promoted from notInTop to top. Now, we'll demote it back to notInTop
			delete(crdt.elems, crdt.smallestScore.Id)
			notTop, has = crdt.notInTop[crdt.smallestScore.Id]
			if !has {
				notTop = make(setTopKElement)
				crdt.notInTop[crdt.smallestScore.Id] = notTop
			}
			notTop.add(crdt.smallestScore)
		}
		//Update the min
		crdt.smallestScore = effect.oldMin
	}
}

func (crdt *TopKRmvCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

/*
func (crdt *TopKRmvCrdt) applyRemove(op *DownstreamTopKRemove) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	remEffect := TopKRemoveEffect{id: op.Id, notTopRemoved: make(setTopKElement), oldMin: crdt.smallestScore}

	rems, hasRems := crdt.rems[op.Id]
	if !hasRems {
		crdt.rems[op.Id] = op.Vc
	} else {
		remEffect.previousVc = rems.Copy()
		crdt.rems[op.Id] = rems.Merge(op.Vc)
	}

	//Find all adds for op.Id that are in notInTop whose ts is < clock[replicaID of the rmv] and remove them.
	hiddenForId := crdt.notInTop[op.Id]
	for elem := range hiddenForId {
		if elem.Ts < op.Vc.GetPos(elem.ReplicaID) {
			delete(hiddenForId, elem)
			remEffect.notTopRemoved.add(elem)
		}
	}
	//Id was totally removed
	if hiddenForId != nil && len(hiddenForId) == 0 {
		delete(crdt.notInTop, op.Id)
	}
	//Also remove from elems if the same condition is true.
	if elem, hasElem := crdt.elems[op.Id]; hasElem && elem.Ts < op.Vc.GetPos(elem.ReplicaID) {
		delete(crdt.elems, op.Id)
		remEffect.remElem = elem

		if len(crdt.elems) == crdt.maxElems-1 {
			//Top-k was full previously, need to find the next highest value to replace (possibly in different ID)
			highestElem := TopKElement{}
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
			if (highestElem != TopKElement{}) {
				//Promote to top-k and set as min
				crdt.elems[highestElem.Id] = highestElem
				crdt.smallestScore = highestElem
				otherDownstreamArgs = DownstreamTopKAdd{TopKElement: highestElem}
				remEffect.minAddedToTop = true
			} else if elem == crdt.smallestScore {
				//No elem to add to top-k, however the removed element was the min. Need to find new one
				crdt.findAndUpdateMin()
			}
		} else if elem == crdt.smallestScore {
			//Removed element was the min, need to find new min
			crdt.findAndUpdateMin()
		}

	}

	var eff Effect = remEffect
	return &eff, otherDownstreamArgs
}
*/

//Protobuf functions
func (crdtOp TopKAdd) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	add := protobuf.GetTopkrmvop().GetAdds()[0]
	crdtOp.TopKScore = TopKScore{Id: add.GetPlayerId(), Score: add.GetScore()}
	return crdtOp
}

func (crdtOp TopKAdd) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Topkrmvop: &proto.ApbTopkRmvUpdate{
		Adds: []*proto.ApbIntPair{&proto.ApbIntPair{PlayerId: pb.Int32(crdtOp.Id), Score: pb.Int32(crdtOp.Score)}},
	}}
}

func (crdtOp TopKRemove) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Id = protobuf.GetTopkrmvop().GetRems()[0]
	return crdtOp
}

func (crdtOp TopKRemove) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Topkrmvop: &proto.ApbTopkRmvUpdate{Rems: []int32{crdtOp.Id}}}
}

func (crdtState TopKValueState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	protoScores := protobuf.GetTopk().GetValues()
	if protoScores == nil {
		//Partial read
		protoScores = protobuf.GetPartread().GetTopk().GetPairs().GetValues()
	}
	crdtState.Scores = make([]TopKScore, len(protoScores))
	for i, pair := range protoScores {
		crdtState.Scores[i] = TopKScore{Id: pair.GetPlayerId(), Score: pair.GetScore()}
	}
	return crdtState
}

func (crdtState TopKValueState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	protos := make([]*proto.ApbIntPair, len(crdtState.Scores))
	for i, score := range crdtState.Scores {
		protos[i] = &proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(score.Score)}
	}
	return &proto.ApbReadObjectResp{Topk: &proto.ApbGetTopkResp{Values: protos}}
}

func (args GetTopNArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.NumberEntries = protobuf.GetTopk().GetGetn().GetAmount()
	return args
}

func (args GetTopKAboveValueArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.MinValue = protobuf.GetTopk().GetGetabovevalue().GetMinValue()
	return args
}

func (args GetTopNArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Topk: &proto.ApbTopkPartialRead{Getn: &proto.ApbTopkGetNRead{
		Amount: pb.Int32(args.NumberEntries),
	}}}
}

func (args GetTopKAboveValueArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Topk: &proto.ApbTopkPartialRead{Getabovevalue: &proto.ApbTopkAboveValueRead{
		MinValue: pb.Int32(args.MinValue),
	}}}
}

func (downOp DownstreamTopKAdd) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	addProto := protobuf.GetTopkrmvOp().GetAdds()[0]
	downOp.Id, downOp.Score, downOp.Ts, downOp.ReplicaID = addProto.GetId(), addProto.GetScore(), addProto.GetTs(), int16(addProto.GetReplicaID())
	return downOp
}

func (downOp DownstreamTopKRemove) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	remProto := protobuf.GetTopkrmvOp().GetRems()[0]
	downOp.Id, downOp.Vc = remProto.GetId(), clocksi.ClockSiTimestamp{}.FromBytes(remProto.GetVc())
	return downOp
}

func (downOp DownstreamTopKAdd) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{TopkrmvOp: &proto.ProtoTopKRmvDownstream{Adds: []*proto.ProtoTopKElement{&proto.ProtoTopKElement{
		Id: pb.Int32(downOp.Id), Score: pb.Int32(downOp.Score), Ts: pb.Int64(downOp.Ts), ReplicaID: pb.Int32(int32(downOp.ReplicaID)),
	}}}}
}

func (downOp DownstreamTopKRemove) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{TopkrmvOp: &proto.ProtoTopKRmvDownstream{Rems: []*proto.ProtoTopKIdVc{&proto.ProtoTopKIdVc{
		Id: pb.Int32(downOp.Id), Vc: downOp.Vc.ToBytes(),
	}}}}
}
