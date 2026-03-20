package crdt

import (
	"container/heap"
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"
	"slices"
	"sort"

	tools "github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

//Note: This idea of the extended top doesn't seem to work without a notInTop :(
//TL:DR of why: consider a top with maxElems=5, maxNotTopElems=15, and the following scores (ignore ID for simplicity): 100, 99, 98, ..., 81.
//If a concurrent rem(95) (which is not on top) and add(80) are executed, and add(80) arrives first (except on the replica originating the rem), those replicas will discard add(80), but the removing replica will not.
//So, as soon as the 80 should be visible on top (e.g., if more rems are issued, even with adds interleaved), the replicas will be inconsistent and a wrong result will be shown.

/*
Note: If replicas may often concurrently add the same ID, and the total top is not small (> 100 elements), this CRDT may underperform. Consider using TopKRmv instead.
Check the discussion right below for more details (including how to optimize for this scenario)

Problem: We need to keep multiple entries for the same ID, in order to ensure correctness.
E.g., consider (1, 100) is on the top. Assume min is < 99, or that the top isn't full.
Then, a concurrent remove(1) and add(1, 99) are issued, and that the add arrives first.
The add would be discarded (< 100, and no way to keep two elements in the same map), and then after remove(1) arrives, the element would be gone.
When in fact, the correct action, would had been for (1, 99) to appear on the top.

Possible solutions:
- Keep elems as map[int32]setTopKElement. OK-ish (as this set is a slice), but increases memory usage, makes access even slower and difficults code.
- Keep an extra map[int32]setTopKElement. Better for memory usage, but implies checking two maps on updates when the element exists on the 1st map.
- Keep a tools.SliceMap[int32, setTopKElement]. Avoids overhead of map most often (as usually the slice will be empty or with very few elements), but it's basically a slice of slices. Ugh
- Third solution is the best. When you think about it, even 1st and 2nd are a map of slices so... a slice of slices is OK.
This solution assumes that usually replicas won't concurrently add the same ID often, as otherwise 1st or 2nd would be better solutions.
*/

//I think wisest strategy is as follows.
//Keep smallestScores buffer (of the extended top), so that we can quickly know if a new element should belong to the top or not, even if there's removes.
//When a read is executed, cache the visible top and keep the min (single) of the visible top.
//The cache will stay valid until an update adds/removes a score that is >= than this min, at which point it is invalidated.
//Only reads will rebuilt the cache (and the single min of the visible top), except for an update that is considered to be initializing TopK's data.

// This TopK CRDT has a single structure to hold all elements.
// In elems, it holds the (visible) top elements, and also an extended (not visible) top elements.
// The idea is to be more storage-efficient than TopKRmv and ensure causal visibility under add-dominated workloads.
// Both the size of the visible and extended tops can be controlled, helping on ensure causal visibility even when removes aren't rare.
// Note that this Top does not have optDownstream, as any element that wouldn't enter the visible or extended top is immediately discarded.
// Finally, note that this TopKRmv is intended for add-dominated workloads (more common than remove-dominated). For non-add-dominated workloads, use TopKRmv.
// This Top guarantees full correctness, including causal visibility, as long as removes are scarse and we don't remove more than maxNotTopElems without readding something to the top (either visible or extended).
// So, in certain skewed workloads, or remove-heavy workloads, incorrectness could be observed, due to discarded adds.
// This kind of situations is detectable and a full recalculation could be issued.
// However, for remove-heavy workloads or skewed workloads, please use TopKRmv instead, which will always ensure eventual consistency. Note that the size of notInTop can also be controlled in TopKRmv.
type TopKRmvExtTopCrdt struct {
	CRDTVM
	vc clocksi.Timestamp //Logical timestamp (each replica uses a counter). So the size of each entry is irrelevant (i.e., no overflow issues.)
	//Metrics to decide if we should cache a read result or not. TopN is always cached as it requires computing the sorted set.
	//Note: only updates that modify the actual top-K count, as only those can invalidate the cache.
	nReads, nUpds int32

	//Visible size of TopK, and actual TopKSize
	maxElems, maxNotTopElems int
	smallestScores           minBuffer[TopKElement] //Buffer with the smallest scores of the extended top.

	elems            map[int32]TopKElement                 //Elements belonging to either the visible top or extended top. (Other adds are discarded)
	rems             map[int32]clocksi.Timestamp           //Removes of all elements that were ever in the visible or extended top.
	duplicateEntries tools.SliceMap[int32, setTopKElement] //Used to keep concurrent, duplicate adds for elems. Check discussion at the top of the file for an explanation why this is needed.

	sortedElems         []TopKScore
	visibleMin          TopKScore
	isTopFull           bool   //True if both visible and extended top are full. Avoids having to check len(elems) == maxElems + maxNotTopElems.
	nClkUpdsSinceLastGC uint16 //Counts the number of times vc was updated since last GC clean. It's okay to overflow this value.
	nMin                int    //Counters for debugging purposes.
}

const DEFAULT_TOPKRMVEXT_FACTOR = 3

type TopKRmvExtTopInit struct {
	TopSize, ExtendedTopSize uint32
}

type DownstreamTopKRmvExtAdd TopKElement
type DownstreamTopKRmvExtRem struct {
	Id int32
	Vc clocksi.Timestamp
}
type DownstreamTopKRmvExtAddAll []TopKElement
type DownstreamTopKRmvExtRemAll struct {
	Vc       clocksi.Timestamp
	DownRems []int32
}

//Effects

type TopKRmvExtTopAddEffect struct {
	newElem, removedMin TopKElement
	oldTs               int64
}

type TopKRmvExtTopReplaceEffect struct {
	newElem, oldElem TopKElement
	oldTs            int64
}

type TopKRmvExtTopDuplicateEffect struct {
	newElem TopKElement
	oldTs   int64
}

// This effect only occurs when we replace an existing ID with a higher score (but same ID).
type TopKRmvExtTopReplaceWithCleanEffect struct {
	TopKRmvExtTopReplaceEffect
	cleanEntries []TopKElement //Note: ID is the same as newElem.Id
}

type TopKRmvExtTopDuplicateWithCleanEffect struct {
	TopKRmvExtTopDuplicateEffect
	cleanEntries []TopKElement //Note: ID is the same as newElem.Id
}

type TopKRmvExtTopRemoveEffect struct {
	id           int32
	previousVc   clocksi.Timestamp //nil in case there wasn't a previous remove for this Id
	remElem      TopKElement
	duplicateRem setTopKElement
}

type TopKRmvExtTopRemoveWithCleanEffect struct {
	TopKRmvExtTopRemoveEffect
	cleanRems []tools.Pair[int32, clocksi.Timestamp] //ID and its remove VC (i.e., key+value of crdt.rems)
}

type TopKRmvExtTopAddAllEffect struct {
	effects      []Effect
	cleanEntries []TopKElement
}
type TopKRmvExtTopRemoveAllEffect struct {
	effects   []TopKRmvExtTopRemoveEffect
	cleanRems []tools.Pair[int32, clocksi.Timestamp] //ID and its remove VC (i.e., key+value of crdt.rems)
}

func (crdt *TopKRmvExtTopCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV_EXT }
func (crdt *TopKRmvExtTopCrdt) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

// Ops
func (args TopKRmvExtTopInit) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV_EXT }
func (args TopKRmvExtTopInit) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

// Downstreams
func (args DownstreamTopKRmvExtAdd) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV_EXT }
func (args DownstreamTopKRmvExtAddAll) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_TOPK_RMV_EXT
}
func (args DownstreamTopKRmvExtRem) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK_RMV_EXT }
func (args DownstreamTopKRmvExtRemAll) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_TOPK_RMV_EXT
}
func (args DownstreamTopKRmvExtAdd) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }
func (args DownstreamTopKRmvExtAddAll) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args DownstreamTopKRmvExtRem) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }
func (args DownstreamTopKRmvExtRemAll) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args DownstreamTopKRmvExtAdd) MustReplicate() bool            { return true }
func (args DownstreamTopKRmvExtAddAll) MustReplicate() bool         { return true }
func (args DownstreamTopKRmvExtRem) MustReplicate() bool            { return true }
func (args DownstreamTopKRmvExtRemAll) MustReplicate() bool         { return true }
func (args TopKRmvExtTopInit) MustReplicate() bool                  { return true }

func (crdt *TopKRmvExtTopCrdt) Initialize(startTs *clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	return crdt.InitializeWithSize(startTs, replicaID, defaultTopKSize)
}

func (crdt *TopKRmvExtTopCrdt) InitializeWithSize(startTs *clocksi.Timestamp, replicaID uint16, size int) (newCrdt CRDT) {
	crdt = &TopKRmvExtTopCrdt{
		CRDTVM:           (&genericInversibleCRDT{}).initialize(crdt),
		vc:               clocksi.NewSliceTimestamp(),
		maxElems:         size,
		maxNotTopElems:   size * DEFAULT_TOPKRMVEXT_FACTOR,
		visibleMin:       MIN_SCORE,
		elems:            make(map[int32]TopKElement, size),
		rems:             make(map[int32]clocksi.Timestamp),
		duplicateEntries: tools.NewSliceMap[int32, setTopKElement](size + size*DEFAULT_TOPKRMVEXT_FACTOR),
		isTopFull:        false,
	}
	crdt.initializeBuffers()
	newCrdt = crdt
	return
}

func (crdt *TopKRmvExtTopCrdt) initializeBuffers() {
	crdt.smallestScores = newMinBuffer(tools.Max(minBufferSize, int(float64(crdt.maxElems)*MIN_BUF_FACTOR)), MIN_ELEM)
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *TopKRmvExtTopCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID uint16) (sameCRDT *TopKRmvExtTopCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *TopKRmvExtTopCrdt) CRDTGC(safeClk clocksi.Timestamp) {
	//A good opportunity to clean rems. However, we will still need to generate an effect with them, as safeClk may be < this CRDT's latest clock.
	//Maybe we can ask CRDVM for this?
	if crdt.nClkUpdsSinceLastGC > 0 { //There was updates since last GC, so attempting a clean is worthwhile.
		cleaned := crdt.cleanupRems()
		latestClk := crdt.CRDTVM.GetLatestClk()
		if !latestClk.IsLowerOrEqual(safeClk) { //Must generate an effect here.
			var effect Effect = TopKRmvRemoveAllWithCleanEffect{cleanedRems: cleaned}
			var fakeUpd DownstreamArguments = DownstreamTopKRemoveAll{}
			crdt.addToHistory(&latestClk, &fakeUpd, &effect)
		} //Else: no need to store effect.
		crdt.nClkUpdsSinceLastGC = 0
	}
}

func (crdt *TopKRmvExtTopCrdt) IsBigCRDT() bool { return crdt.maxElems > 100 && len(crdt.elems) > 100 }

func (crdt *TopKRmvExtTopCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	crdt.nReads++
	if crdt.sortedElems == nil && (crdt.nUpds <= 1 || (crdt.nReads)/(crdt.nUpds+1) > 10) { //nUpds+1 to be safe on the case there's no updates yet. nUpds <= 1 to account for initial data setting (usually with TopKAddAll)
		crdt.makeSortedElems()
	}
	//TODO: Consider updsNotYetApplied in all of these
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case GetTopNArguments:
		return crdt.getTopN(typedArgs.NumberEntries, updsNotYetApplied)
	case GetTopKAboveValueArguments:
		return crdt.getTopKAboveValue(typedArgs.MinValue, updsNotYetApplied)
	case TopAggregateArguments:
		return crdt.getTopAggregate(typedArgs.MinValue, typedArgs.MaxValue, typedArgs.Bitmask, typedArgs.AggregateType, updsNotYetApplied)
	default:
		fmt.Printf("[TOPKRmvExtTopCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *TopKRmvExtTopCrdt) makeSortedElems() {
	//Better make a copy with all elements and sort it, other than trying to keep only the top elements (and then having to recalculate min and still sort at the end).
	values := make([]TopKScore, len(crdt.elems))
	i := 0
	for _, elem := range crdt.elems {
		values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
		i++
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Score > values[j].Score })
	i = tools.Min(i, crdt.maxElems)
	if len(values) > 2*crdt.maxElems { //Make a smaller copy to save memory.
		crdt.sortedElems = make([]TopKScore, i)
		copy(crdt.sortedElems, values[:i])
	} else {
		crdt.sortedElems = values[:i]
	}
	crdt.visibleMin = values[i-1]
}

func (crdt *TopKRmvExtTopCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	if crdt.sortedElems == nil { //Make sortedElems, as we need to find out the actual top.
		crdt.makeSortedElems()
	}
	values := make([]TopKScore, len(crdt.sortedElems))
	copy(values, crdt.sortedElems)
	return TopKValueState{Scores: values}
}

func (crdt *TopKRmvExtTopCrdt) getTopN(numberEntries int32, updsNotYetApplied []UpdateArguments) (state State) {
	if crdt.sortedElems == nil { //Make sortedElems, as we need to find out the actual top.
		crdt.makeSortedElems()
	}
	if numberEntries > int32(len(crdt.sortedElems)) {
		numberEntries = int32(len(crdt.sortedElems))
	}
	values := make([]TopKScore, numberEntries)
	copy(values, crdt.sortedElems[:numberEntries])
	return TopKValueState{Scores: values}
}

func (crdt *TopKRmvExtTopCrdt) getTopKAboveValue(minValue int32, updsNotYetApplied []UpdateArguments) (state State) {
	var values []TopKScore
	if crdt.sortedElems != nil { //Faster if it is available.
		if !crdt.smallestScores.hasMin() || minValue <= crdt.smallestScores.getMin().Score { //If it doesn't have min, the top is empty. So we can use this codepath.
			values = make([]TopKScore, len(crdt.elems))
			copy(values, crdt.sortedElems)
		} else if len(crdt.sortedElems) > 200 { //Attempt to find the end position and use a direct copy (faster)
			//Binary search + copy.
			left := 0
			right := len(crdt.sortedElems) - 1
			for left <= right {
				mid := (left + right) / 2
				if crdt.sortedElems[mid].Score >= minValue {
					left = mid + 1
				} else {
					right = mid - 1
				}
			}
			values = make([]TopKScore, left)
			copy(values, crdt.sortedElems[:left])
		} else { //Just iterate and copy manually.
			values = make([]TopKScore, len(crdt.elems))
			for i, elem := range crdt.sortedElems {
				if elem.Score >= minValue {
					//values[actuallyAdded] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
					values[i] = elem //This will copy as its a value type.
				} else {
					values = values[:i]
					break
				}
			}
		}
		return TopKValueState{Scores: values}
	} else { //Copy only the values above minValue.
		values = make([]TopKScore, len(crdt.elems))
		i := 0
		for _, elem := range crdt.elems {
			if elem.Score >= minValue {
				values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
				i++
			}
		}
		values = values[:i]
		sort.Slice(values, func(i, j int) bool { return values[i].Score > values[j].Score })
		if i >= crdt.maxElems { //This is actually sortedElems! Store it.
			crdt.visibleMin = values[crdt.maxElems-1]
			crdt.sortedElems = make([]TopKScore, crdt.maxElems) //Store a copy with the right size.
			copy(crdt.sortedElems, values[:crdt.maxElems])
			return TopKValueState{Scores: values[:crdt.maxElems]} //Return original.
		} else {
			return TopKValueState{Scores: values} //Number of elems is < maxElems, so we just return directly.
		}
	}
}

func (crdt *TopKRmvExtTopCrdt) getTopAggregate(minValue, maxValue int32, bitmask int32, aggrType AggregateType, updsNotYetApplied []UpdateArguments) (state State) {
	if len(crdt.elems) == 0 {
		return getAggregateState(aggrType, 0)
	}
	minScore := crdt.smallestScores.getMin().Score
	if aggrType == M_MIN && minScore > minValue { //This is already known.
		return getAggregateState(aggrType, int64(minScore))
	}
	if crdt.sortedElems == nil { //We have to find what's the actual top. So, better force calculating sortedElems first.
		crdt.makeSortedElems()
	}
	if aggrType == M_MAX && crdt.sortedElems[0].Score < maxValue { //This is already known.
		return getAggregateState(aggrType, int64(crdt.sortedElems[0].Score))
	}
	return aggrStrategyChooserSortedElems(crdt.sortedElems, crdt.sortedElems[0].Score, minScore, minValue, maxValue, bitmask, aggrType)
}

func (crdt *TopKRmvExtTopCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
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
	case TopKAddAll:
		downstreamArgs = crdt.getTopKAddAllDownstreamArgs(&opType)
	case TopKRemoveAll:
		downstreamArgs = crdt.getTopKRemoveAllDownstreamArgs(&opType)
	case TopKRmvExtTopInit:
		downstreamArgs = crdt.getInitDownstreamArgs(opType)
	case MultiUpd:
		multiDowns := make(MultiUpd, len(opType))
		for i, innerUpd := range opType {
			multiDowns[i] = crdt.Update(innerUpd)
		}
		return multiDowns
	default:
		fmt.Printf("[TopKRmvExtTop][Update]Unknown update type: %v (%T)\n", args, args)
	}
	return
}

func (crdt *TopKRmvExtTopCrdt) getInitDownstreamArgs(initOp TopKRmvExtTopInit) (args TopKRmvExtTopInit) {
	if len(crdt.elems) == 0 { //Set nElems immediately if it's the first op, in order for upcoming Update() to make correct decisions. This does not affect correctness.
		crdt.maxElems, crdt.maxNotTopElems = int(initOp.TopSize), int(initOp.ExtendedTopSize)
	}
	return initOp
}

func (crdt *TopKRmvExtTopCrdt) getTopKAddDownstreamArgs(addOp *TopKAdd) (args DownstreamArguments) {
	myReplicaID := shared.SortedReplicaID
	crdt.vc.SelfIncTimestamp(myReplicaID)
	//All entries will be replicated, unless the top is full and the score is deemed small enough.
	minElem := crdt.getMinWhenMaybeEmpty()
	if crdt.isTopFull && addOp.Score < minElem.Score/2 { //Optimization: if the extended top is full, and this score is very small, discard it.
		return NoOp{}
	}

	localArgs := DownstreamTopKRmvExtAdd(TopKElement{Id: addOp.Id, Score: addOp.Score, TsId: makeTsWithReplicaID(crdt.vc.GetPos(myReplicaID), myReplicaID), Data: addOp.Data})
	if addOp.Data == nil {
		localArgs.Data = emptyData
	}
	//Even if we already have the ID, we still want to replicate in case a concurrent remove happens. Note that before we already check if the score is too small to be relevant.
	return localArgs
}

func (crdt *TopKRmvExtTopCrdt) getMinWhenMaybeEmpty() TopKElement {
	if crdt.smallestScores.hasMin() {
		return crdt.smallestScores.getMin()
	}
	return MIN_ELEM
}

// A few points worth noting/current limitations:
// Doesn't support "Opt". That is, all adds which are for fault tolerance purposes only are ignored.
// The current workings of this method lead to more adds than necessary being downstreamed, since the old min is always used as a reference.
// For the latter, the same exact problem happens when executing multiple TopKAdd in the same txn.
func (crdt *TopKRmvExtTopCrdt) getTopKAddAllDownstreamArgs(addOp *TopKAddAll) (args DownstreamArguments) {
	myReplicaID := shared.SortedReplicaID
	crdt.vc.SelfIncTimestamp(myReplicaID)
	localClk := crdt.vc.GetPos(myReplicaID)
	tsId := makeTsWithReplicaID(localClk, myReplicaID)
	nAdd := 0
	var downAdds []TopKElement
	currElem := TopKElement{TsId: tsId, Data: emptyData}
	if len(crdt.elems) == 0 { //Initialization. No need to check min score or existing elements
		totalMax := crdt.maxElems + crdt.maxNotTopElems
		if len(addOp.Scores) <= int(float64(totalMax)*1.5) { //Straight-forward copy
			downAdds = make([]TopKElement, len(addOp.Scores))
			for i, add := range addOp.Scores {
				currElem.Id, currElem.Score = add.Id, add.Score
				if add.Data != nil {
					currElem.Data = add.Data
				}
				downAdds[i] = currElem
				currElem.Data = emptyData //Reset
			}
			return DownstreamTopKRmvExtAddAll(downAdds)
		} //else: better filter only the elements that will go to the top+extended top.
		if totalMax <= 100 && len(addOp.Scores) >= totalMax*5 { //Use a maxBuffer.
			maxBuf := newMaxBuffer(totalMax, MIN_ELEM)
			for _, score := range addOp.Scores {
				currElem.Id, currElem.Score = score.Id, score.Score
				if score.Data != nil {
					currElem.Data = score.Data
				}
				maxBuf.addIfInBetween(currElem, maxBuf.Len()) //Passing maxBuf.Len() ensures that if the buffer isn't full, even new "mins" will be added to the buffer.
				currElem.Data = emptyData                     //Reset
			}
			return DownstreamTopKRmvExtAddAll(maxBuf.maxs)
		} else { //Better copy everything, sort and then filter. Note that we already know that len(addOp.Scores) is, at least, 1.5*(maxElems + maxNotTopElems)
			downAdds = make([]TopKElement, len(addOp.Scores))
			for i, add := range addOp.Scores {
				currElem.Id, currElem.Score = add.Id, add.Score
				if add.Data != nil {
					currElem.Data = add.Data
				}
				downAdds[i] = currElem
				currElem.Data = emptyData //Reset
			}
			sort.Slice(downAdds, func(i, j int) bool { return downAdds[i].isHigher(downAdds[j]) })
			return DownstreamTopKRmvExtAddAll(downAdds[:totalMax])
		}
	}

	//Since this is not initialization, we will assume not many elements are being added.
	downAdds = make([]TopKElement, len(addOp.Scores))
	minElem := crdt.smallestScores.getMin()
	for _, add := range addOp.Scores {
		if crdt.isTopFull && add.Score < minElem.Score/2 { //Optimization: if the extended top is full, and this score is very small, discard it.
			continue
		}
		currElem = TopKElement{Id: add.Id, Score: add.Score, TsId: tsId, Data: add.Data}
		if add.Data == nil {
			currElem.Data = emptyData
		}
		downAdds[nAdd] = currElem
		nAdd++
	}
	return DownstreamTopKRmvExtAddAll(downAdds[:nAdd])
}

func (crdt *TopKRmvExtTopCrdt) getTopKRemoveDownstreamArgs(remOp *TopKRemove) (args DownstreamArguments) {
	//Check if ID is in the top (either visible of extended)
	_, inElems := crdt.elems[remOp.Id]
	if !inElems {
		//This remove is irrelevant as this ID isn't in the top and never will be until a more recent add appears
		args = NoOp{}
	} else {
		//Remove for element in top-k. Must be propagated
		//Ensuring that the VC of the update and of the CRDT are different instances in order to avoid modifying the upd's accidentally.
		args = DownstreamTopKRmvExtRem{Id: remOp.Id, Vc: crdt.vc.Copy()}
	} //Even if the element is on the extended top, we will replicate the remove.
	return
}

func (crdt *TopKRmvExtTopCrdt) getTopKRemoveAllDownstreamArgs(remOp *TopKRemoveAll) (args DownstreamArguments) {
	inElems := false
	nRem, nOpt := 0, 0
	removesDown := DownstreamTopKRmvExtRemAll{DownRems: make([]int32, len(remOp.Ids))}

	for _, id := range remOp.Ids {
		_, inElems = crdt.elems[id]
		if inElems {
			removesDown.DownRems[nRem] = id
			nRem++
		}
		//else: Irrelevant, and this ID isn't in the top (neither visible nor extended) and never will be until a more revent add appears
	}
	if nRem == 0 && nOpt == 0 {
		return NoOp{}
	}
	removesDown.Vc, removesDown.DownRems = crdt.vc.Copy(), removesDown.DownRems[:nRem]
	return removesDown
}

func (crdt *TopKRmvExtTopCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		//fmt.Printf("[TOPKRMV]Original args: %v (%T). Converted args: %v.\n", downstreamArgs, downstreamArgs, multiUpd)
		//fmt.Printf("[TOPKRMV]Original args type: %T.\n", downstreamArgs)
		var otherDown MultiUpd
		for _, innerDown := range multiUpd {
			newDown := crdt.Downstream(updTs, innerDown.(DownstreamArguments))
			if newDown != nil {
				otherDown = append(otherDown, newDown)
			}
		}
		return otherDown
	}
	effect, otherDownstreamArgs := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)
	return
}

func (crdt *TopKRmvExtTopCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	switch opType := downstreamArgs.(type) {
	case DownstreamTopKRmvExtAdd:
		effect = crdt.applyAdd(opType)
	case DownstreamTopKRmvExtRem:
		effect = crdt.applyRemove(opType)
	case DownstreamTopKRmvExtAddAll:
		effect = crdt.applyAddAll(opType)
	case DownstreamTopKRmvExtRemAll:
		effect = crdt.applyRemoveAll(opType)
	case TopKRmvExtTopInit:
		effect = crdt.applyInit(opType)
	default:
		fmt.Printf("[TopKRmvExtTop][Downstream]Unsupported downstream type %v (%T)\n", downstreamArgs, downstreamArgs)
	}
	return
}

func (crdt *TopKRmvExtTopCrdt) applyInit(op TopKRmvExtTopInit) (effect *Effect) {
	if op.ExtendedTopSize <= 0 {
		op.ExtendedTopSize = op.TopSize * DEFAULT_TOPKRMVEXT_FACTOR
	}
	if int(op.TopSize+op.ExtendedTopSize) > (crdt.maxElems+crdt.maxNotTopElems)*10 && len(crdt.elems) == 0 {
		crdt.elems = make(map[int32]TopKElement, int(op.TopSize)) //Resize.
	}
	duplicateEntries := tools.NewSliceMap[int32, setTopKElement](int(op.TopSize) + int(op.ExtendedTopSize)) //Since it is a slice, it must be always resized.
	crdt.maxElems, crdt.maxNotTopElems = int(op.TopSize), int(op.ExtendedTopSize)
	crdt.initializeBuffers()
	var effectValue Effect = NoEffect{}
	effect = &effectValue
	if len(crdt.elems) > 0 { //This is outside Init's intended usage.
		for i := 0; i < crdt.duplicateEntries.Len(); i++ {
			key, value := crdt.duplicateEntries.GetByPos(i)
			duplicateEntries.SetOnPos(key, value, i)
		}
		crdt.findAndUpdateMin()
	}
	crdt.duplicateEntries = duplicateEntries
	return
}

func (crdt *TopKRmvExtTopCrdt) applyAdd(op DownstreamTopKRmvExtAdd) (effect *Effect) {
	//Mostly the same as in TopKRmv.
	var effectValue Effect = NoEffect{}
	opTs, opReplicaID := op.TsId.getTs(), op.TsId.getReplicaID()
	oldTs := crdt.vc.GetPos(opReplicaID)
	crdt.vc.UpdatePos(opReplicaID, opTs)
	crdt.nClkUpdsSinceLastGC++
	opElem := TopKElement(op)

	remsVc, hasEntry := crdt.rems[op.Id]
	if !hasEntry || remsVc.GetPos(opReplicaID) < opTs {
		elem, hasId := crdt.elems[op.Id]
		if hasId {
			//Check if the "new elem" is > elem. If it is, add it. On the end, check if min should be updated.
			if opElem.isHigher(elem) {
				effectValue = TopKRmvExtTopReplaceEffect{newElem: opElem, oldElem: elem, oldTs: oldTs}
				crdt.elems[op.Id] = opElem
				if elem.TsId.getReplicaID() != opReplicaID {
					//Store old value in duplicateEntries, in case of a concurrent remove.
					clean := crdt.addAndCleanDuplicateEntries(elem, clocksi.GetNumberReplicas())
					if len(clean) > 0 {
						effectValue = TopKRmvExtTopReplaceWithCleanEffect{TopKRmvExtTopReplaceEffect: effectValue.(TopKRmvExtTopReplaceEffect), cleanEntries: clean}
					}
				}
				//Update min if needed
				crdt.smallestScores.remove(elem)
				crdt.smallestScores.addIfInBetween(opElem, len(crdt.elems)) //This will never kick out any existing elem, as op > elem: if op belongs there, so did elem.
				if !crdt.smallestScores.hasMin() {
					crdt.findAndUpdateMin()
				}
				if crdt.visibleMin.Score < op.Score {
					//Idea: We only erase sortedElems if the newElem would go to the visible top.
					crdt.sortedElems, crdt.visibleMin, crdt.nUpds = nil, MIN_SCORE, crdt.nUpds+1
				}
			} else {
				//Store value in duplicateEntries, in case of a concurrent remove.
				clean := crdt.addAndCleanDuplicateEntries(opElem, clocksi.GetNumberReplicas())
				if len(clean) > 0 {
					effectValue = TopKRmvExtTopDuplicateWithCleanEffect{TopKRmvExtTopDuplicateEffect: TopKRmvExtTopDuplicateEffect{newElem: opElem, oldTs: oldTs}, cleanEntries: clean}
				} else {
					effectValue = TopKRmvExtTopDuplicateEffect{newElem: opElem, oldTs: oldTs}
				}
			}
		} else { //Note: From now on, we know this ID doesn't exist in the top yet.
			//Check if it should belong to elems (i.e., there's space or its score is > min)
			if !crdt.isFull() {
				crdt.elems[op.Id] = opElem
				//Check if min should be updated
				removedMin := crdt.smallestScores.addIfInBetween(opElem, len(crdt.elems))
				effectValue = TopKRmvExtTopAddEffect{newElem: opElem, removedMin: removedMin, oldTs: oldTs}
				if crdt.isFull() {
					crdt.isTopFull = true
				}
			} else if opElem.isHigher(crdt.smallestScores.getMin()) {
				oldMin := crdt.smallestScores.removeMin()
				effectValue = TopKRmvExtTopReplaceEffect{newElem: opElem, oldElem: oldMin, oldTs: oldTs}
				//Get rid of this element and also from duplicateEntries, if it exists there.
				delete(crdt.elems, oldMin.Id)
				crdt.duplicateEntries.Delete(oldMin.Id)
				crdt.elems[opElem.Id] = opElem
				crdt.smallestScores.addIfInBetween(opElem, len(crdt.elems))
				if !crdt.smallestScores.hasMin() {
					crdt.findAndUpdateMin()
				}
			} //Else: elem is too small, it won't belong to even the extended top. Ignore it. No effect.
			if crdt.visibleMin.Score < opElem.Score { //Despite the top not being full, maybe the visible top was already full and this element goes to extended top.
				crdt.sortedElems, crdt.visibleMin, crdt.nUpds = nil, MIN_SCORE, crdt.nUpds+1
			}
		}
	} //Else: A more recent remove exists. Not a problem, this remove will be propagated by who originated it.
	return &effectValue
}

func (crdt *TopKRmvExtTopCrdt) applyAddAll(op DownstreamTopKRmvExtAddAll) (effect *Effect) {
	//Mostly the same as in TopKRmv.
	listEffects := tools.NewSliceWithCounter[Effect](len(op))
	firstElem := op[0]
	oldTs := crdt.vc.GetPos(firstElem.TsId.getReplicaID())
	crdt.vc.UpdatePos(firstElem.TsId.getReplicaID(), firstElem.TsId.getTs())
	crdt.nClkUpdsSinceLastGC++
	nReplicas := clocksi.GetNumberReplicas()
	//If it's nil (or not full), we can act as if the top was changed. It won't have any impact but avoids some extra checks when applying the op.
	changedTop := (crdt.sortedElems == nil || len(crdt.elems) < crdt.maxElems)

	//There's no removes to propagate, as all operations are immediately propagated if relevant.
	//Note: all elems already have the data field set.
	for _, newElem := range op {
		remsVc, hasEntry := crdt.rems[newElem.Id]
		newElemTs, newElemReplicaID := newElem.TsId.getTs(), newElem.TsId.getReplicaID()
		if !hasEntry || remsVc.GetPos(newElemReplicaID) < newElemTs {
			elem, hasId := crdt.elems[newElem.Id]
			if hasId {
				//Check if the "new elem" is > elem. If it is, add it. On the end, check if min should be updated.
				if newElem.isHigher(elem) {
					listEffects.AddToEnd(TopKRmvExtTopReplaceEffect{newElem: newElem, oldElem: elem, oldTs: oldTs})
					crdt.elems[newElem.Id] = newElem
					if elem.TsId.getReplicaID() != newElemReplicaID {
						//Store old value in duplicateEntries, in case of a concurrent remove.
						crdt.addToDuplicateEntries(elem, nReplicas)
					}
					//Update min if needed
					crdt.smallestScores.remove(elem)
					crdt.smallestScores.addIfInBetween(newElem, len(crdt.elems)) //This will never kick out any existing elem, as newElem > elem: if newElem belongs there, so did elem.
					if !crdt.smallestScores.hasMin() {
						crdt.findAndUpdateMin()
					}
					if !changedTop && crdt.visibleMin.Score < newElem.Score {
						//Idea: We only erase sortedElems if the newElem would go to the visible top.
						crdt.sortedElems, changedTop, crdt.visibleMin = nil, true, MIN_SCORE
					}
				} else {
					//Store value in duplicateEntries, in case of a concurrent remove.
					//TODO: Which Effect?
					crdt.addToDuplicateEntries(newElem, nReplicas)
					listEffects.AddToEnd(TopKRmvExtTopDuplicateEffect{newElem: newElem, oldTs: oldTs})
				}
			} else { //Note: From now on, we know this ID doesn't exist in the top yet.
				//Check if it should belong to elems (i.e., there's space or its score is > min)
				if !crdt.isFull() {
					crdt.elems[newElem.Id] = newElem
					//Check if min should be updated
					removedMin := crdt.smallestScores.addIfInBetween(newElem, len(crdt.elems))
					listEffects.AddToEnd(TopKRmvExtTopAddEffect{newElem: newElem, removedMin: removedMin, oldTs: oldTs})
					if crdt.isFull() {
						crdt.isTopFull = true
					}
				} else if newElem.isHigher(crdt.smallestScores.getMin()) {
					oldMin := crdt.smallestScores.removeMin()
					listEffects.AddToEnd(TopKRmvExtTopReplaceEffect{newElem: newElem, oldElem: oldMin, oldTs: oldTs})
					//Get rid of this element and also from duplicateEntries, if it exists there.
					delete(crdt.elems, oldMin.Id)
					crdt.duplicateEntries.Delete(oldMin.Id)
					crdt.elems[newElem.Id] = newElem
					crdt.smallestScores.addIfInBetween(newElem, len(crdt.elems))
					if !crdt.smallestScores.hasMin() {
						crdt.findAndUpdateMin()
					}
				} //Else: elem is too small, it won't belong to even the extended top. Ignore it. No effect.
				if !changedTop && crdt.visibleMin.Score < newElem.Score { //Despite the top not being full, maybe the visible top was already full and this element goes to extended top.
					crdt.sortedElems, changedTop, crdt.visibleMin = nil, true, MIN_SCORE
				}
			}
		} //Else: A more recent remove exists. Not a problem, this remove will be propagated by who originated it.
		//TODO: Effects. Also take into consideration NoEffect{}, as they're common here.
	}
	if changedTop {
		crdt.nUpds++
	}
	if crdt.sortedElems == nil && len(op) >= 100 { //Likely a initialization operation. We'll make the sortedElems now to avoid the overhead when reading.
		crdt.makeSortedElems()
	}
	cleanDuplicate := crdt.cleanUneededDuplicateEntries()
	addAllEffect := TopKRmvExtTopAddAllEffect{cleanEntries: cleanDuplicate}
	if listEffects.Len() < listEffects.Cap()/10 { //Copy to a new slice to reduce memory usage.
		copy := listEffects.Copy()
		addAllEffect.effects = copy.ToSlice()
	} else {
		addAllEffect.effects = listEffects.ToSlice()
	}
	var effectValue Effect = addAllEffect
	return &effectValue
}

func (crdt *TopKRmvExtTopCrdt) applyRemove(op DownstreamTopKRmvExtRem) (effect *Effect) {
	remEffect := TopKRmvExtTopRemoveEffect{id: op.Id}
	rems, hasRems := crdt.rems[op.Id]
	if !hasRems {
		remEffect.previousVc = nil
		crdt.rems[op.Id] = op.Vc
	} else {
		remEffect.previousVc = rems.Copy()
		crdt.rems[op.Id] = rems.Merge(op.Vc)
	}
	elem, hasElem := crdt.elems[op.Id]
	if hasElem {
		removedFromTop := false //Idea: if we remove from top, we may need to replace from duplicateEntries.
		if elem.TsId.getTs() < op.Vc.GetPos(elem.TsId.getReplicaID()) {
			removedFromTop = true
			delete(crdt.elems, op.Id)
			remEffect.remElem = elem
			if elem.Score >= crdt.visibleMin.Score {
				//Only erase sortedElems if the removed element was in the visible top.
				crdt.sortedElems, crdt.visibleMin, crdt.nUpds = nil, MIN_SCORE, crdt.nUpds+1
			}
			crdt.smallestScores.remove(elem)
			if !crdt.smallestScores.hasMin() {
				crdt.findAndUpdateMin()
			}
			crdt.isTopFull = false //Everytime we remove an element it won't be full anymore.
		}
		if entries, pos := crdt.duplicateEntries.GetReturnPos(elem.Id); pos != tools.NOT_FOUND {
			remEffect.duplicateRem = tools.NewSliceSet[TopKElement](entries.Len())
			entriesSlice := entries.GetValues()
			highestSurvivor := MIN_ELEM
			for i := len(entriesSlice) - 1; i >= 0; i-- { //Backwards iteration so that it's safe to do deletes.
				entry := entriesSlice[i]
				if entry.TsId.getTs() < crdt.vc.GetPos(entry.TsId.getReplicaID()) {
					entries.DeleteByPos(i) //Whichever element replaces this position, it will be from the front, which we already processed.
					remEffect.duplicateRem.AddNoCheck(entry)
				} else if removedFromTop && entry.isHigher(highestSurvivor) {
					highestSurvivor = entry
				}
			}
			if entries.Len() == 0 {
				crdt.duplicateEntries.DeleteByPos(pos)
			} else if removedFromTop { //We found for sure an element that is candidate to the top (as entries.Len() is still above 0.)
				//Add this element to the top, as now we have a space for sure.
				crdt.elems[highestSurvivor.Id] = highestSurvivor
				crdt.smallestScores.addIfInBetween(highestSurvivor, len(crdt.elems))
				if crdt.isFull() {
					crdt.isTopFull = true
				}
			}
		}
	} //Else: nothing further to do (other than effect.)
	if len(crdt.rems) > 5*(crdt.maxElems+crdt.maxNotTopElems) {
		removed := crdt.cleanupRems()
		if len(removed) > 0 {
			remWithClean := TopKRmvExtTopRemoveWithCleanEffect{TopKRmvExtTopRemoveEffect: remEffect, cleanRems: removed}
			var effectValue Effect = remWithClean
			return &effectValue
		}
	}
	var effectValue Effect = remEffect
	return &effectValue
}

func (crdt *TopKRmvExtTopCrdt) applyRemoveAll(op DownstreamTopKRmvExtRemAll) (effect *Effect) {
	//If it's nil (or not full), we can act as if the top was changed. It won't have any impact but avoids some extra checks when applying the op.
	changedTop := (crdt.sortedElems == nil || len(crdt.elems) < crdt.maxElems)
	listEffect := TopKRmvExtTopRemoveAllEffect{effects: make([]TopKRmvExtTopRemoveEffect, len(op.DownRems))}
	var currEffect TopKRmvExtTopRemoveEffect
	for i, remID := range op.DownRems {
		currEffect.id = remID
		rems, hasRems := crdt.rems[remID]
		if !hasRems {
			currEffect.previousVc = nil
			crdt.rems[remID] = op.Vc
		} else {
			currEffect.previousVc = rems.Copy()
			crdt.rems[remID] = rems.Merge(op.Vc)
		}
		elem, hasElem := crdt.elems[remID]
		if hasElem {
			removedFromTop := false //Idea: if we remove from top, we may need to replace from duplicateEntries.
			if elem.TsId.getTs() < op.Vc.GetPos(elem.TsId.getReplicaID()) {
				removedFromTop = true
				delete(crdt.elems, remID)
				currEffect.remElem = elem
				if !changedTop && elem.Score >= crdt.visibleMin.Score {
					//Only erase sortedElems if the removed element was in the visible top.
					crdt.sortedElems, changedTop, crdt.visibleMin = nil, true, MIN_SCORE
				}
				crdt.smallestScores.remove(elem)
				if !crdt.smallestScores.hasMin() {
					crdt.findAndUpdateMin()
				}
				crdt.isTopFull = false //Everytime we remove an element it won't be full anymore.
			}
			if entries, pos := crdt.duplicateEntries.GetReturnPos(elem.Id); pos != tools.NOT_FOUND {
				currEffect.duplicateRem = tools.NewSliceSet[TopKElement](entries.Len())
				entriesSlice := entries.GetValues()
				highestSurvivor := MIN_ELEM
				for i := len(entriesSlice) - 1; i >= 0; i-- { //Backwards iteration so that it's safe to do deletes.
					entry := entriesSlice[i]
					if entry.TsId.getTs() < crdt.vc.GetPos(entry.TsId.getReplicaID()) {
						entries.DeleteByPos(i) //Whichever element replaces this position, it will be from the front, which we already processed.
						currEffect.duplicateRem.AddNoCheck(entry)
					} else if removedFromTop && entry.isHigher(highestSurvivor) {
						highestSurvivor = entry
					}
				}
				if entries.Len() == 0 {
					crdt.duplicateEntries.DeleteByPos(pos)
				} else if removedFromTop { //We found for sure an element that is candidate to the top (as entries.Len() is still above 0.)
					//Add this element to the top, as now we have a space for sure.
					crdt.elems[highestSurvivor.Id] = highestSurvivor
					crdt.smallestScores.addIfInBetween(highestSurvivor, len(crdt.elems))
					if crdt.isFull() {
						crdt.isTopFull = true
					}
				}
			}
		} //else: nothing further to do (other than effect.)
		listEffect.effects[i] = currEffect
	}
	if changedTop {
		crdt.nUpds++
	}
	if len(crdt.rems) > 5*(crdt.maxElems+crdt.maxNotTopElems) {
		listEffect.cleanRems = crdt.cleanupRems()
	}
	var effectValue Effect = listEffect
	return &effectValue
}

// This will be called by remove operations when crdt.rems' size exceeds considerably the size of the visible+extended top.
// Idea: if crdt.vc has advanced enough, old removes can be discarded, as causal consistency ensures we won't receive any older add.
func (crdt *TopKRmvExtTopCrdt) cleanupRems() (removed []tools.Pair[int32, clocksi.Timestamp]) {
	tmpRem := tools.NewSliceWithCounter[tools.Pair[int32, clocksi.Timestamp]](len(crdt.rems) / 2) //At worst one resize.
	for id, remsVc := range crdt.rems {
		if remsVc.IsLower(crdt.vc) {
			tmpRem.Append(tools.Pair[int32, clocksi.Timestamp]{First: id, Second: remsVc})
			delete(crdt.rems, id)
		}
	}
	if tmpRem.Len() < len(crdt.rems)/10 { //Copy to avoid wasting too much memory
		removed = make([]tools.Pair[int32, clocksi.Timestamp], tmpRem.Len())
		copy(removed, tmpRem.ToSlice())
	} else {
		removed = tmpRem.ToSlice()
	}
	crdt.nClkUpdsSinceLastGC = 0 //We already cleaned rems, so GC doesn't need to clean it.
	return
}

func (crdt *TopKRmvExtTopCrdt) addToDuplicateEntries(elem TopKElement, nReplicas int) {
	entry, has := crdt.duplicateEntries.Get(elem.Id)
	if !has {
		entry = tools.NewSliceSet[TopKElement](nReplicas)
		crdt.duplicateEntries.SetNew(elem.Id, entry)
	}
	entry.AddNoCheck(elem)
}

// Used only by single add. The idea is that, in the meantime, other adds may have updated VC enough for us to know some existing elements aren't needed anymore.
func (crdt *TopKRmvExtTopCrdt) addAndCleanDuplicateEntries(elem TopKElement, nReplicas int) (clean []TopKElement) {
	entry, has := crdt.duplicateEntries.Get(elem.Id)
	if !has { //In this case there's nothing to clean. The new element should always be added to duplicate.
		entry = tools.NewSliceSet[TopKElement](nReplicas)
		crdt.duplicateEntries.SetNew(elem.Id, entry)
		entry.AddNoCheck(elem)
		return nil
	} else {
		existingValues := entry.GetValues()
		clean = make([]TopKElement, len(existingValues))
		j := 0
		for i := entry.Len() - 1; i >= 0; i-- { //Backwards iteration so that it's safe to do deletes.
			existingEntry := existingValues[i]
			if existingEntry.TsId.getTs() < crdt.vc.GetPos(existingEntry.TsId.getReplicaID()) {
				clean[j] = existingEntry
				j++
				entry.DeleteByPos(i) //Whichever element replaces this position, it will be from the front, which we already processed.
			}
		}
		entry.AddNoCheck(elem)
		return clean[:j]
	}
}

// Iterates duplicateEntries to see if there's entries no longer needed, by comparing with crdt.vc
func (crdt *TopKRmvExtTopCrdt) cleanUneededDuplicateEntries() (removedDuplicates []TopKElement) {
	remDuplicates := tools.NewSliceWithCounter[TopKElement](crdt.duplicateEntries.Len()) //Size may not be enough depending on how many duplicates in each position we have.
	keys, values := crdt.duplicateEntries.GetKeys(), crdt.duplicateEntries.GetValues()
	for i, key := range keys {
		entries := values[i]
		entriesSlice := entries.GetValues()
		safe := true
		//Backwards iteration so that it's safe to do deletes.
		for i := len(entriesSlice) - 1; i >= 0; i-- {
			entry := entriesSlice[i]
			if entry.TsId.getTs() >= crdt.vc.GetPos(entry.TsId.getReplicaID()) {
				safe = false
			} else { //Can at least delete this one.
				entries.DeleteByPos(i) //Whichever element replaces this position, it will be from the front, which we already processed.
				remDuplicates.Append(entry)
			}
		}
		if safe {
			crdt.duplicateEntries.Delete(key)
		}
	}
	if remDuplicates.Len() == 0 {
		return
	} else if remDuplicates.Len() < remDuplicates.Cap()/10 { //Resize to save memory.
		removedDuplicates = make([]TopKElement, remDuplicates.Len())
		copy(removedDuplicates, remDuplicates.ToSlice())
	}
	return remDuplicates.ToSlice()
}

// This will be inlined.
func (crdt *TopKRmvExtTopCrdt) isFull() bool {
	return len(crdt.elems) == crdt.maxElems+crdt.maxNotTopElems
}

func (crdt *TopKRmvExtTopCrdt) findAndUpdateMin() {
	if len(crdt.elems) <= 1500 { //Cheaper to copy to a slice and then sort.
		elemsSlice := make([]TopKElement, len(crdt.elems))
		i := 0
		for _, elem := range crdt.elems {
			elemsSlice[i] = elem
			i++
		}
		slices.SortFunc(elemsSlice, func(a, b TopKElement) int {
			if a.Score != b.Score {
				return int(a.Score - b.Score)
			}
			return int(a.Id - b.Id)
		})
		crdt.smallestScores.copyFrom(elemsSlice)
	} else { //Need a heap.
		bufSize := crdt.smallestScores.Cap()
		h := newMaxHeapTopKElem(bufSize)
		heap.Init(h)
		for _, elem := range crdt.elems {
			if h.Len() < bufSize {
				heap.Push(h, elem)
			} else if elem.isSmaller((*h)[0]) {
				heap.Pop(h)
				heap.Push(h, elem)
			}
		}
		minSlice := crdt.smallestScores.mins[:bufSize]
		for i := len(minSlice) - 1; i >= 0; i-- {
			minSlice[i] = heap.Pop(h).(TopKElement)
		}
		crdt.smallestScores.size = bufSize
	}
	crdt.nMin++
}

func (crdt *TopKRmvExtTopCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

//METHODS FOR INVERSIBLE CRDT

func (crdt *TopKRmvExtTopCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := TopKRmvExtTopCrdt{
		CRDTVM:           crdt.CRDTVM.copy(),
		vc:               crdt.vc.Copy(),
		maxElems:         crdt.maxElems,
		maxNotTopElems:   crdt.maxNotTopElems,
		smallestScores:   crdt.smallestScores.copy(),
		elems:            tools.MapCopy(crdt.elems),
		rems:             tools.MapCopy(crdt.rems),
		duplicateEntries: *(crdt.duplicateEntries.Copy().(*tools.SliceMap[int32, setTopKElement])),
		nUpds:            crdt.nUpds,
		nReads:           crdt.nReads,
		isTopFull:        crdt.isTopFull,
	}
	return &newCrdt
}

func (crdt *TopKRmvExtTopCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *TopKRmvExtTopCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	effect, _ = crdt.applyDownstream(updArgs)
	return
}

func (crdt *TopKRmvExtTopCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case TopKRmvExtTopAddEffect:
		crdt.undoAddEffect(typedEffect)
	case TopKRmvExtTopReplaceEffect:
		crdt.undoReplaceEffect(typedEffect)
	case TopKRmvExtTopDuplicateEffect:
		crdt.undoAddDuplicateEffect(typedEffect)
	case TopKRmvExtTopRemoveEffect:
		crdt.undoRemoveEffect(typedEffect)
	case TopKRmvExtTopAddAllEffect:
		crdt.undoAddAllEffect(typedEffect)
	case TopKRmvExtTopRemoveAllEffect:
		crdt.undoRemoveAllEffect(typedEffect)
	case TopKRmvExtTopReplaceWithCleanEffect:
		crdt.undoReplaceEffect(typedEffect.TopKRmvExtTopReplaceEffect)
		crdt.undoCleanDuplicates(typedEffect.cleanEntries)
	case TopKRmvExtTopDuplicateWithCleanEffect:
		crdt.undoAddDuplicateEffect(typedEffect.TopKRmvExtTopDuplicateEffect)
		crdt.undoCleanDuplicates(typedEffect.cleanEntries)
	case TopKRmvExtTopRemoveWithCleanEffect:
		crdt.undoRemoveEffect(typedEffect.TopKRmvExtTopRemoveEffect)
		crdt.undoCleanRems(typedEffect.cleanRems)
	}
}

func (crdt *TopKRmvExtTopCrdt) undoAddAllEffect(effect TopKRmvExtTopAddAllEffect) {
	for _, eff := range effect.effects {
		switch typedEffect := eff.(type) {
		case TopKRmvExtTopAddEffect:
			crdt.undoAddEffect(typedEffect)
		case TopKRmvExtTopReplaceEffect:
			crdt.undoReplaceEffect(typedEffect)
		case TopKRmvExtTopDuplicateEffect:
			crdt.undoAddDuplicateEffect(typedEffect)
		}
	}
	if len(effect.cleanEntries) > 0 { //Note: we can't use undoCleanDuplicates here, as there's many different IDs. Also, the same ID may appear a few times in a row.
		prevId := MIN_ELEM.Id
		var currEntry setTopKElement
		var has bool
		for _, elem := range effect.cleanEntries {
			if prevId != elem.Id {
				//Get or create entry for this ID
				currEntry, has = crdt.duplicateEntries.Get(elem.Id)
				if !has {
					currEntry = tools.NewSliceSet[TopKElement](clocksi.GetNumberReplicas())
					crdt.duplicateEntries.SetNew(elem.Id, currEntry)
				}
				prevId = elem.Id
			}
			currEntry.AddNoCheck(elem)
		}
	}
}

func (crdt *TopKRmvExtTopCrdt) undoRemoveAllEffect(effect TopKRmvExtTopRemoveAllEffect) {
	for _, eff := range effect.effects {
		crdt.undoRemoveEffect(eff)
	}
	if len(effect.cleanRems) > 0 {
		crdt.undoCleanRems(effect.cleanRems)
	}
}

func (crdt *TopKRmvExtTopCrdt) undoAddEffect(effect TopKRmvExtTopAddEffect) {
	crdt.vc.UpdateForcedPos(effect.newElem.TsId.getReplicaID(), effect.oldTs)
	//Element was added to the top and there wasn't any entry for it previously on top. Also, there was space in crdt.elems.
	delete(crdt.elems, effect.newElem.Id)
	if effect.removedMin != MIN_ELEM {
		crdt.smallestScores.remove(effect.removedMin)
	}
}

func (crdt *TopKRmvExtTopCrdt) undoReplaceEffect(effect TopKRmvExtTopReplaceEffect) {
	crdt.vc.UpdateForcedPos(effect.newElem.TsId.getReplicaID(), effect.oldTs)
	//Element was added to the top.
	//Two possibilities: we replaced the same ID (and thus the old one got added to duplicate), or we replaced a different ID.
	delete(crdt.elems, effect.newElem.Id)
	crdt.smallestScores.remove(effect.newElem)
	crdt.smallestScores.addIfInBetween(effect.oldElem, len(crdt.elems))
	crdt.elems[effect.oldElem.Id] = effect.oldElem
	if effect.oldElem.Id == effect.newElem.Id { //Same ID, remove oldElem from duplicates
		entry := crdt.duplicateEntries.GetDirect(effect.oldElem.Id)
		entry.Delete(effect.oldElem)
		if entry.Len() == 0 {
			crdt.duplicateEntries.Delete(effect.oldElem.Id)
		}
	}
}

func (crdt *TopKRmvExtTopCrdt) undoAddDuplicateEffect(effect TopKRmvExtTopDuplicateEffect) {
	crdt.vc.UpdateForcedPos(effect.newElem.TsId.getReplicaID(), effect.oldTs)
	crdt.duplicateEntries.GetDirect(effect.newElem.Id).Delete(effect.newElem)
}

func (crdt *TopKRmvExtTopCrdt) undoCleanDuplicates(clean []TopKElement) {
	//The ID is always the same.
	id := clean[0].Id
	entries, has := crdt.duplicateEntries.Get(id)
	if !has {
		entries = tools.NewSliceSet[TopKElement](clocksi.GetNumberReplicas())
		crdt.duplicateEntries.SetNew(id, entries)
	}
	for _, elem := range clean {
		entries.AddNoCheck(elem)
	}
}

func (crdt *TopKRmvExtTopCrdt) undoRemoveEffect(effect TopKRmvExtTopRemoveEffect) {
	if effect.previousVc == nil { //This was the first remove for this item. So we can delete its entry from crdt.rems
		delete(crdt.rems, effect.id)
	} else {
		crdt.rems[effect.id] = effect.previousVc.Copy()
	}

	nReplicas := clocksi.GetNumberReplicas()
	if !effect.remElem.isEqual(TopKElement{}) {
		crdt.elems[effect.id] = effect.remElem
		crdt.smallestScores.addIfInBetween(effect.remElem, len(crdt.elems))
	}
	if effect.duplicateRem != nil { //It's possible this happens even if remElem is empty, as the ts may be too small to remove the top element but may remove some duplicates.
		entry, has := crdt.duplicateEntries.Get(effect.id)
		if !has {
			entry = tools.NewSliceSet[TopKElement](nReplicas)
			crdt.duplicateEntries.SetNew(effect.id, entry)
		}
		for _, elem := range effect.duplicateRem.GetValues() {
			entry.AddNoCheck(elem)
		}
	}
}

func (crdt *TopKRmvExtTopCrdt) undoCleanRems(clean []tools.Pair[int32, clocksi.Timestamp]) {
	for _, pair := range clean {
		crdt.rems[pair.First] = pair.Second
	}
}

func (crdt *TopKRmvExtTopCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

// Protobuf functions
// Other than the initializer, the add/rem operations (update version) are the same as TopK. Downstreams are different though.
func (crdtOp TopKRmvExtTopInit) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	init := protobuf.GetTopkinitop()
	crdtOp.TopSize, crdtOp.ExtendedTopSize = uint32(init.GetTopSize()), uint32(init.GetNotTopSize()) //Even if init.GetNotTopSize() is undefined, it is safe as 0 will be returned and we handle that correctly.
	return crdtOp
}

func (crdtOp TopKRmvExtTopInit) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	initPb := &proto.ApbTopKInit{TopSize: &crdtOp.TopSize, TopType: proto.CRDTType_TOPK_RMV_EXT.Enum()}
	if crdtOp.ExtendedTopSize > 0 {
		initPb.NotTopSize = &crdtOp.ExtendedTopSize
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkinitop{Topkinitop: initPb}}
}

// We also re-use the states and queries from TopKRmv.

func (downOp DownstreamTopKRmvExtAdd) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	addProto := protobuf.GetTopkrmvOp().GetAdds()[0]
	downOp.Id, downOp.Score, downOp.TsId, downOp.Data = addProto.GetId(), addProto.GetScore(), tsWithReplicaID(addProto.GetTsId()), DataHelper(addProto.GetData())
	return downOp
}

func (downOp DownstreamTopKRmvExtAdd) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	addProto := &proto.ProtoTopKElement{Id: &downOp.Id, Score: &downOp.Score, TsId: pb.Uint64(uint64(downOp.TsId))}
	if downOp.Data != nil && len(*downOp.Data) > 0 {
		addProto.Data = *downOp.Data
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkrmvOp{TopkrmvOp: &proto.ProtoTopKRmvDownstream{Adds: []*proto.ProtoTopKElement{addProto}}}}
}

func (downOp DownstreamTopKRmvExtRem) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	remProto := protobuf.GetTopkrmvOp().GetRems()
	downOp.Id, downOp.Vc = remProto.GetIds()[0], clocksi.SliceTimestamp{}.FromBytes(remProto.GetVcs()[0])
	return downOp
}

func (downOp DownstreamTopKRmvExtRem) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkrmvOp{TopkrmvOp: &proto.ProtoTopKRmvDownstream{
		Rems: &proto.ProtoTopKRmvRemove{Ids: []int32{downOp.Id}, Vcs: [][]byte{downOp.Vc.ToBytes()}}}}}
}

func (downOp DownstreamTopKRmvExtAddAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	addProto := protobuf.GetTopkrmvOp().GetAdds()
	downOp = make([]TopKElement, len(addProto))
	var curr TopKElement
	for i, add := range addProto {
		curr.Id, curr.Score, curr.TsId, curr.Data = add.GetId(), add.GetScore(), tsWithReplicaID(add.GetTsId()), DataHelper(add.GetData())
		downOp[i] = curr
	}
	return downOp
}

func (downOp DownstreamTopKRmvExtAddAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoElems := make([]*proto.ProtoTopKElement, len(downOp))
	for i, add := range downOp {
		curr := proto.ProtoTopKElement{Id: pb.Int32(add.Id), Score: pb.Int32(add.Score), TsId: pb.Uint64(uint64(add.TsId))}
		if add.Data != nil && len(*add.Data) > 0 {
			curr.Data = *add.Data
		}
		protoElems[i] = &curr
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkrmvOp{TopkrmvOp: &proto.ProtoTopKRmvDownstream{Adds: protoElems}}}
}

func (downOp DownstreamTopKRmvExtRemAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	remProto := protobuf.GetTopkrmvOp().GetRems()
	ids, vcs := remProto.GetIds(), remProto.GetVcs()
	downOp.DownRems, downOp.Vc = ids, clocksi.SliceTimestamp{}.FromBytes(vcs[0]) //There's always only one vc.
	return downOp
}

func (downOp DownstreamTopKRmvExtRemAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoVcs := [][]byte{downOp.Vc.ToBytes()} //There's always only one vc.
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkrmvOp{TopkrmvOp: &proto.ProtoTopKRmvDownstream{
		Rems: &proto.ProtoTopKRmvRemove{Ids: downOp.DownRems, Vcs: protoVcs}}}}
}

func (crdtOp TopKRmvExtTopInit) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	init := protobuf.GetTopkinitOp()
	crdtOp.TopSize, crdtOp.ExtendedTopSize = init.GetTopSize(), init.GetNotTopSize() //If notTop is undefined, 0 will be returned, which we correctly handle.
	return crdtOp
}

func (crdtOp TopKRmvExtTopInit) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	initPb := &proto.ProtoTopKInitDownstream{TopSize: pb.Uint32(crdtOp.TopSize)}
	if crdtOp.ExtendedTopSize > 0 {
		initPb.NotTopSize = pb.Uint32(crdtOp.ExtendedTopSize)
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkinitOp{TopkinitOp: initPb}}
}

func (crdt *TopKRmvExtTopCrdt) GetCRDT() CRDT { return crdt }
