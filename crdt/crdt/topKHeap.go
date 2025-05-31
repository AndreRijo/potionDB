package crdt

//TODO: Effects, Add/RemoveAll, etc.

import (
	"container/heap"
	"math"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
)

type TopKHeapCrdt struct {
	CRDTVM
	vc        clocksi.Timestamp
	replicaID int16

	//Max number of elements that can be in TopK
	maxElems int
	//Elements that are in the top-K
	elems     map[int32]*TopKHeapElement
	elemsHeap TopHeap
	//Removes of all elements, both in and not in top-K
	rems map[int32]clocksi.Timestamp
	//Elems for which either:
	//a) the value is < smallestScore and thus shouldn't be in top
	//b) the value is >= smallestScore yet a higher value for the same ID is already on top, albeit with a smaller TS.
	notInTop     map[int32]*setTopKHeapElement
	notInTopHeap NotInTopHeap
	//A copy of elemsHeap already prepared to be returned in queries. Gets invalidated when elems is updated.
	sortedElems []TopKScore
}

type TopKHeapElement struct {
	TopKElement
	pos    int            //Position in the heap.
	notTop setTopKElement //Other adds
}

type setTopKHeapElement struct {
	set map[TopKElement]struct{}
	max TopKElement //Highest element in this set; needed for the heap's scoring.
	pos int         //Position in the heap
}

func makeSetTopKHeapElement() *setTopKHeapElement {
	return &setTopKHeapElement{set: make(map[TopKElement]struct{})}
}

// Adds an element to the set. This hides the internal representation of the set
func (set setTopKHeapElement) add(elem TopKElement) (updated bool) {
	set.set[elem] = struct{}{}
	if elem.isHigher(set.max) {
		set.max = elem
		return true
	}
	return false
}

// Search for max after removing elements
func (set setTopKHeapElement) findMax() (updated bool) {
	oldMax := set.max
	set.max = minTopKElement
	for elem := range set.set {
		if elem.isHigher(set.max) {
			set.max = elem
		}
	}
	return set.max != oldMax //Max changed
}

func (set setTopKHeapElement) isHigher(otherSet setTopKHeapElement) bool {
	return set.max.isHigher(otherSet.max)
}

func (set setTopKHeapElement) copy() (newSet setTopKHeapElement) {
	newSet = setTopKHeapElement{set: make(map[TopKElement]struct{}), max: set.max, pos: set.pos}
	for id, elem := range set.set {
		newSet.set[id] = elem
	}
	return
}

// Uses a bound slice, so push and pop are more efficient. Values must be initialized with a size.
type TopHeap struct {
	values   []*TopKHeapElement
	nEntries *int
}

func (h TopHeap) Len() int { return len(h.values) }

// We want the heap's Pop() to return the lowest element, so we use < on "less". The smallest element is on h[0].
func (h TopHeap) Less(i, j int) bool { return h.values[j].isHigher(h.values[i].TopKElement) }

func (h TopHeap) Swap(i, j int) {
	h.values[i], h.values[j] = h.values[j], h.values[i]
	h.values[i].pos, h.values[j].pos = i, j
}

func (h TopHeap) Push(value interface{}) {
	convValue := value.(*TopKHeapElement)
	convValue.pos = *h.nEntries
	h.values[*h.nEntries], *h.nEntries = convValue, *h.nEntries+1

}

func (h TopHeap) Pop() interface{} {
	if *h.nEntries == 0 {
		return nil
	}
	old := h.values[*h.nEntries-1]
	h.values[*h.nEntries-1] = nil
	*h.nEntries--
	return old
}

// Returns the highest value, but does not remove it.
func (h TopHeap) Peek() TopKElement {
	if *h.nEntries == 0 {
		return TopKElement{}
	}
	return h.values[0].TopKElement
}

// Replaces an existing TopKElement. (Unused so far)
func (h TopHeap) Update(newElem TopKElement, pos int) {
	h.values[pos].TopKElement = newElem
	heap.Fix(h, pos)
}

// This probably needs more to it as there may be multiple entries for the same element. We'll see
// Uses an unbound slice, so push and pop are less efficient.
type NotInTopHeap []*setTopKHeapElement

func (h NotInTopHeap) Len() int { return len(h) }

// We want the heap's Pop() to return the highest element, so we use >. The highest element is in h[0].
func (h NotInTopHeap) Less(i, j int) bool { return h[i].isHigher(*h[j]) }

func (h NotInTopHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h NotInTopHeap) Push(value interface{}) {
	convValue := value.(*setTopKHeapElement)
	convValue.pos = len(h)
	h = append(h, convValue)
}

func (h NotInTopHeap) Pop() interface{} {
	if len(h) == 0 {
		return nil
	}
	slice := h
	toRemove := slice[len(slice)-1]
	slice[len(slice)-1] = nil
	//Must decrease the slice size too
	h = slice[0 : len(slice)-1]
	return toRemove
}

var minTopKElement = TopKElement{Id: math.MinInt32, Score: math.MinInt32} //For comparison purposes

func (crdt *TopKHeapCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV }

func (crdt *TopKHeapCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return crdt.InitializeWithSize(startTs, replicaID, defaultTopKSize)
}

func (crdt *TopKHeapCrdt) InitializeWithSize(startTs *clocksi.Timestamp, replicaID int16, size int) (newCrdt CRDT) {
	crdt = &TopKHeapCrdt{
		CRDTVM:       (&genericInversibleCRDT{}).initialize(startTs, nil, nil, nil), //TODO
		vc:           clocksi.NewClockSiTimestamp(),
		replicaID:    replicaID,
		maxElems:     size,
		elems:        make(map[int32]*TopKHeapElement),
		elemsHeap:    TopHeap{values: make([]*TopKHeapElement, size), nEntries: new(int)},
		rems:         make(map[int32]clocksi.Timestamp),
		notInTop:     make(map[int32]*setTopKHeapElement),
		notInTopHeap: make(NotInTopHeap, 0, size),
	}
	newCrdt = crdt
	return
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *TopKHeapCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *TopKHeapCrdt) {
	crdt.CRDTVM, crdt.replicaID = (&genericInversibleCRDT{}).initialize(startTs, nil, nil, nil), replicaID //TODO
	return crdt
}

func (crdt *TopKHeapCrdt) IsBigCRDT() bool { return crdt.maxElems > 100 && len(crdt.elems) > 100 }

func (crdt *TopKHeapCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	//TODO: Consider updsNotYetApplied in all of these
	/*
		switch typedArgs := args.(type) {
		case StateReadArguments:
			return crdt.getState(updsNotYetApplied)
		case GetTopNArguments:
			return crdt.getTopN(typedArgs.NumberEntries, updsNotYetApplied)
		case GetTopKAboveValueArguments:
			return crdt.getTopKAboveValue(typedArgs.MinValue, updsNotYetApplied)
		default:
			fmt.Printf("[TOPKCrdt]Unknown read type: %+v\n", args)
		}
		return nil
	*/
	return nil
}

func (crdt *TopKHeapCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	//add, rmv: needs to be propagated. add_r, rmv_r - *may* need to be propagated later depending on the operations that are executed in other replicas.
	//or actually, the _r version may be the one that is replicated to the other replicas for fault tolerance? Need to ask that
	//E.g of add_r: consider the operations: add(10, 10, 0), add(10, 5, 5). Now consider that another replica did rem(10) after receiving the first add.
	//In this case, the 2nd add would then be relevant and need to be propagated.

	/*
		switch opType := args.(type) {
		case TopKAdd:
			downstreamArgs = crdt.getTopKAddDownstreamArgs(&opType)
		case TopKRemove:
			//Ensuring that the VC of the update and of the CRDT are different instances in order to avoid modifying the upd's accidentally.
			downstreamArgs = crdt.getTopKRemoveDownstreamArgs(&opType)
		case TopKAddAll:
			downstreamArgs = crdt.getTopKAddAllDownstreamArgs(&opType)
		case TopKRemoveAll:
			downstreamArgs = crdt.getTopKRemoveAllDownstreamArgs(&opType)
		}
	*/
	return
}

func (crdt *TopKHeapCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect, otherDownstreamArgs := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)
	return
}

func (crdt *TopKHeapCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
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
	case DownstreamTopKAddAll:
		effect, otherDownstreamArgs = crdt.applyAddAll(&opType)
	case DownstreamTopKRemoveAll:
		effect, otherDownstreamArgs = crdt.applyRemoveAll(&opType)
	}
	return
}

// Effect addToTop (include previousMin and previousEntry, if any)?
// Effect addToNotTop
func (crdt *TopKHeapCrdt) applyAdd(op *DownstreamTopKAdd) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	//fmt.Println("Applying topK add")
	var effectValue Effect
	oldTs := crdt.vc.GetPos(op.ReplicaID)
	if op.Data == nil {
		op.Data = &[]byte{}
	}

	crdt.vc.UpdatePos(op.ReplicaID, op.Ts)
	remsVc, hasEntry := crdt.rems[op.Id]
	if !hasEntry || remsVc.GetPos(op.ReplicaID) < op.Ts {
		elem, hasId := crdt.elems[op.Id]
		if hasId {
			//Check if the "new elem" is > elem. If it is, add it.
			if op.TopKElement.isHigher(elem.TopKElement) {
				//fmt.Printf("[TOPK][ADD]Id already exists and new value is higher, number elems %d, max elems %d, min score %d\n", len(crdt.elems), crdt.maxElems, crdt.smallestScore.Score)
				effectValue = TopKRmvReplaceEffect{newElem: op.TopKElement, oldElem: elem.TopKElement, oldTs: oldTs}
				old := elem.TopKElement
				elem.TopKElement = op.TopKElement
				heap.Fix(crdt.elemsHeap, elem.pos) //Update position of element in the heap
				//Different replicas, so we'll keep the old entry (as it might be relevant again depending on concurrent removes)
				if old.ReplicaID != op.TopKElement.ReplicaID {
					elem.notTop.add(old)
				}
				crdt.sortedElems = nil
			} else {
				//fmt.Printf("[TOPK][ADD]Id already exists but new value is lower, number elems %d, max elems %d, min score %d\n", len(crdt.elems), crdt.maxElems, crdt.smallestScore.Score)
				effectValue = TopKRmvAddNotTopEffect{TopKRmvAddEffect: TopKRmvAddEffect{TopKElement: op.TopKElement, oldTs: oldTs}}
				//Store as it might be relevant later on
				elem.notTop.add(op.TopKElement)
			}
		} else {
			//Check if it should belong to topK (i.e. there's space or its score is > min)
			if len(crdt.elems) < crdt.maxElems {
				//fmt.Printf("[TOPK][ADD]Still has space for more elements, number elems %d, max elems %d, min score %d\n", len(crdt.elems), crdt.maxElems, crdt.smallestScore.Score)
				newEntry := &TopKHeapElement{TopKElement: op.TopKElement, notTop: make(setTopKElement)}
				crdt.elems[op.Id] = newEntry
				heap.Push(crdt.elemsHeap, newEntry)
				effectValue = TopKRmvAddEffect{TopKElement: op.TopKElement, oldTs: oldTs}
				crdt.sortedElems = nil
			} else if min := crdt.elemsHeap.Peek(); op.TopKElement.isHigher(min) {
				//fmt.Printf("[TOPK][ADD]Doesn't have space for more elements, but new is higher than smallest score. Number elems %d, max elems %d, min score %d\n", len(crdt.elems), crdt.maxElems, crdt.smallestScore.Score)
				delete(crdt.elems, min.Id)
				minEntry := heap.Pop(crdt.elemsHeap).(*TopKHeapElement)
				effectValue = TopKRmvReplaceEffect{newElem: op.TopKElement, oldElem: min, oldMin: min, oldTs: oldTs}
				//Take min out of top (and store in notInTop) to give space to the new elem with higher score
				minEntry.notTop.add(min) //Need to add the min itself to the set
				crdt.moveToNotInTop(minEntry)
				newElem := &TopKHeapElement{TopKElement: op.TopKElement}
				notTopEntry, hasInNotTop := crdt.notInTop[op.Id]
				//The new elem may already exist in notInTop; if so, we need to gather those entries and move to top too
				if hasInNotTop {
					newElem.notTop = notTopEntry.set
					delete(crdt.notInTop, newElem.Id)
					heap.Remove(crdt.notInTopHeap, notTopEntry.pos)
					notTopEntry = nil
				} else {
					newElem.notTop = make(setTopKElement)
				}
				crdt.elems[op.Id] = newElem
				heap.Push(crdt.elemsHeap, newElem)
				crdt.sortedElems = nil
			} else {
				//fmt.Printf("[TOPK][ADD]Doesn't have space for more elements and new is smaller than smallest score. Number elems %d, max elems %d, min score %d\n", len(crdt.elems), crdt.maxElems, crdt.smallestScore.Score)
				effectValue = TopKRmvAddNotTopEffect{TopKRmvAddEffect: TopKRmvAddEffect{TopKElement: op.TopKElement, oldTs: oldTs}}
				//effectValue = TopKAddEffect{TopKElement: op.TopKElement}
				//Add to notInTop
				crdt.addToNotInTop(op.TopKElement)
			}
		}

	} else {
		//Must return this remove to propagate to other replicas
		otherDownstreamArgs = DownstreamTopKRemove{Id: op.Id, Vc: remsVc}
		effectValue = NoEffect{}
		//fmt.Println("[TOPKRMV]Apply add is returning a new remove.")
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
func (crdt *TopKHeapCrdt) applyRemove(op *DownstreamTopKRemove) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	//Must be <= as the remove's clk is a copy without incrementing.
	remEffect := TopKRmvRemoveEffect{id: op.Id, notTopRemoved: make(setTopKElement)}
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
	if hiddenForId != nil {
		for elem := range hiddenForId.set {
			if elem.Ts <= op.Vc.GetPos(elem.ReplicaID) {
				delete(hiddenForId.set, elem)
				remEffect.notTopRemoved.add(elem)
			}
		}
		//Id was totally removed
		if len(hiddenForId.set) == 0 {
			delete(crdt.notInTop, op.Id)
			heap.Remove(crdt.notInTopHeap, hiddenForId.pos)
		} else {
			//May need to recalculate the position in the heap
			maxChanged := hiddenForId.findMax()
			if maxChanged {
				heap.Fix(crdt.notInTopHeap, hiddenForId.pos)
			}
		}
	}
	//Also remove from elems if the same condition is true.
	if elem, hasElem := crdt.elems[op.Id]; hasElem {
		//Do the same procedure as in "hiddenForId"
		for otherElem := range elem.notTop {
			if otherElem.Ts <= op.Vc.GetPos(otherElem.ReplicaID) {
				delete(elem.notTop, otherElem)
				remEffect.notTopRemoved.add(otherElem)
			}
		}
		if elem.Ts <= op.Vc.GetPos(elem.ReplicaID) {
			delete(crdt.elems, op.Id)
			heap.Remove(crdt.elemsHeap, elem.pos)
			remEffect.remElem = elem.TopKElement
			crdt.sortedElems = nil

			//Need to add leftover entries to notInTop
			if len(elem.notTop) > 0 {
				crdt.moveToNotInTop(elem)
			}

			if len(crdt.notInTop) > 0 {
				//There is a space on top and there are elements not in top.
				toAdd := crdt.notInTopHeap.Pop().(*setTopKHeapElement)
				delete(crdt.notInTop, toAdd.max.Id)
				topElem := &TopKHeapElement{TopKElement: toAdd.max, notTop: toAdd.set}
				delete(topElem.notTop, topElem.TopKElement)
				//Promote to top-k
				crdt.elems[topElem.Id] = topElem
				heap.Push(crdt.elemsHeap, topElem)

				//Need to prepare downstream args of the promoted element
				otherDown := DownstreamTopKAdd{TopKElement: topElem.TopKElement}
				if otherDown.Data == nil {
					otherDown.Data = &[]byte{}
				}
				otherDownstreamArgs = otherDown
			}
		}

	}

	var eff Effect = remEffect
	return &eff, otherDownstreamArgs
}

func (crdt *TopKHeapCrdt) applyAddAll(op *DownstreamTopKAddAll) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	return
}

func (crdt *TopKHeapCrdt) applyRemoveAll(op *DownstreamTopKRemoveAll) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	return
}

func (crdt *TopKHeapCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

/*
type Interface interface {
	sort.Interface
	Push(x any) // add x as element Len()
	Pop() any   // remove and return element Len() - 1.
}

type Interface interface {
	// Len is the number of elements in the collection.
	Len() int

	// Less reports whether the element with index i
	// must sort before the element with index j.
	//
	// If both Less(i, j) and Less(j, i) are false,
	// then the elements at index i and j are considered equal.
	// Sort may place equal elements in any order in the final result,
	// while Stable preserves the original input order of equal elements.
	//
	// Less must describe a transitive ordering:
	//  - if both Less(i, j) and Less(j, k) are true, then Less(i, k) must be true as well.
	//  - if both Less(i, j) and Less(j, k) are false, then Less(i, k) must be false as well.
	//
	// Note that floating-point comparison (the < operator on float32 or float64 values)
	// is not a transitive ordering when not-a-number (NaN) values are involved.
	// See Float64Slice.Less for a correct implementation for floating-point values.
	Less(i, j int) bool

	// Swap swaps the elements with indexes i and j.
	Swap(i, j int)
}
*/

// Auxiliary method to add an element to NotInTop and its respective heap
func (crdt *TopKHeapCrdt) addToNotInTop(elem TopKElement) {
	entry, hasEntry := crdt.notInTop[elem.Id]
	if !hasEntry {
		entry = makeSetTopKHeapElement()
		crdt.notInTop[elem.Id] = entry
	}
	updatedMax := entry.add(elem)
	if !hasEntry {
		//Can only add to the heap after an element is already in the set (max needs to be defined)
		heap.Push(crdt.notInTopHeap, entry)
	} else if updatedMax {
		//Need to update the heap
		heap.Fix(crdt.notInTopHeap, entry.pos)
	}
}

// Similar to addToNotInTop.
// In this case, however, we know there is no entry yet in notInTop, and we possibly have multiple entries of the same ID to add there
func (crdt *TopKHeapCrdt) moveToNotInTop(entry *TopKHeapElement) {
	notTopEntry := &setTopKHeapElement{set: entry.notTop}
	notTopEntry.findMax()
	entry = nil
	heap.Push(crdt.notInTopHeap, notTopEntry)
}

func (crdt *TopKHeapCrdt) GetCRDT() CRDT { return crdt }
