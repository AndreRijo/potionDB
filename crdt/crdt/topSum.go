package crdt

import (
	"container/heap"
	"fmt"
	"math"
	"slices"
	"sort"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"

	//pb "github.com/golang/protobuf/proto"
	tools "github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

/*
MayHaveObservableImpact:
We must keep the sum propragated to other replicas, as well as total sum.

	logCoreLocal: effect-update ops generated at the local replica for which hasImpact() is true, and wasn't yet propagated.
	logLocal: effect-update ops generated at the local replica and not yet propagated
	logrecv: effect-update ops propagated to all replicas, including operations generated locally

For a given id:

	sum(addsNotPropagated) > ((min(top) - (state[i] - sum(addsNotPropagated))) / nReplicas)
	In words:
	The sum of non-propagated adds MUST exceed (diff between lowest top and (diff to state divided by nReplicas))
	...or basically, if every replica did the same amount of "non-propagated" adds, it would belong to the top
*/

//Important note: Always use positive values for add/sub.
//Unlike counter, there's different processing depending if we're incrementing or decrementing.

//TODO: "MixAll"? I.e., adds + subs.
//TODO: Might need to rethink "add" due to negative values in nonProp.

type TopSumCrdt struct {
	CRDTVM

	//Metrics to decide if we should cache a read result or not. TopN is always cached as it requires computing the sorted set.
	//Note: only updates that modify the top-K (i.e., that modify elems) count, as only those can invalidate the cache.
	nReads, nUpds int32

	//Max number of elements that can be in top-K
	maxElems int
	//The "smallest score" in the top-K. Useful to know if when a new add arrives it should be added to the top or "hidden" to notInTop
	//smallestScore *TopKScore
	smallestScores minBuffer[TopKScore]
	highestNotTop  maxBuffer[TopKScore]
	//Elements that are in the top-K
	elems map[int32]*TopKScore
	//Elems which sum is < smallestScore
	notInTop map[int32]*TopKScore
	//Sum of adds not yet propagated.
	notPropagated map[int32]*TopKScore
	//Buffer for queries.
	//Each time the top is modified it gets nilled, and is rebuilt on the first execution of one of those queries.
	//Note that changes to notInTop DOES NOT nil this buffer.
	//Since this buffer is nilled on update (of the top), it is safe to return this slice directly on reads.
	sortedElems      []TopKScore
	nMaxNotTop, nMin int //Counters for debugging purposes
}

type TopSValueState struct {
	Scores []TopKScore
}

type TopSAdd struct {
	TopKScore
}

// Pre: All scores must be non-negative, unless this is the first time the element is being added to the CRDT.
type TopSAddAll struct {
	Scores []TopKScore
}

type TopSSub struct {
	TopKScore
}

// Pre: All scores must be non-negative, unless this is the first time the element is being added to the CRDT.
type TopSSubAll struct {
	Scores []TopKScore
}

type TopSAddAndSubAll struct {
	AddScores []TopKScore
	SubScores []TopKScore
}

type TopSInit uint32

// Just passing the score is enough
// However, we need to keep a bool pointer to know whenever this operation must be replicated or not
// Reason for pointer: structs are copied by value, thus this CRDT receives a copy of the version in materializer.
type DownstreamTopSAdd struct {
	TopKScore
	replicate    *bool
	srcReplicaID uint16 //If it comes from another replica, the processing may be different
}

// Note: Doesn't need replicate *bool as if there's nothing to replicate, len(Scores) == 0.
type DownstreamTopSAddAll struct {
	Scores       *[]TopKScore //Pointer to slice so that we can modify the set of scores AFTER we conclude which ones are to be replicated
	srcReplicaID uint16
}

type DownstreamTopSSub struct {
	TopKScore
	replicate *bool
	//TopSSub doesn't need srcReplicaID as a decrement never triggers a new operation/processing.
}

type DownstreamTopSSubAll struct {
	Scores *[]TopKScore
}

type DownstreamTopSAddAndSubAll struct {
	AddScores    *[]TopKScore //Pointer to slice so that we can modify the set of scores AFTER we conclude which ones are to be replicated
	SubScores    *[]TopKScore
	srcReplicaID uint16
}

type GetTopSumNArguments struct {
	NumberEntries int32
}

type GetTopSumAboveValueArguments struct {
	MinValue int32
}

//Note: notPropagated is ignored when rebuilding versions, as it is assumed only reads are done on old CRDTs.

// Added to top
type TopSumAddEffect struct {
	newScore TopKScore
	oldScore int32
}

type TopSumAddNotTopEffect struct {
	newScore TopKScore
	oldScore int32
}

// A new element was promoted to the top, which led to the old element being moved to nonTop.
type TopSumAddReplaceEffect struct {
	newElem         TopKScore
	newElemOldScore int32     //Might have been an increase
	oldElem         TopKScore //The element that went to notInTop was the previous min
}

// Equal to TopSumAddReplaceEffect, but the swap is instead from top to nonTop. Different processing though.
type TopSumSubReplaceEffect struct {
	newElem         TopKScore //Element that went to notInTop
	newElemOldScore int32     //The element's previous value when it was on top
	oldElem         TopKScore //The element that got promoted from notInTop to Top. It is the current minimum
}

// Used for both TopSumAddAll and TopSumSubAll, as most effects are common between both.
type TopSumMultiEffect []Effect

// Added to top
/*type TopSumAddEffect struct {
	newScore TopKScore
	oldScore int32
	oldMin   TopKScore //Due to increments, the min may change.
}

// Added to notTop
type TopSumAddNotTopEffect struct {
	newScore TopKScore
	oldScore int32
}

// A new element was promoted to the top, which led to the old element being moved to nonTop.
type TopSumAddReplaceEffect struct {
	newElem         TopKScore
	newElemOldScore int32     //Might have been an increase
	oldElem         TopKScore //The element that went to notInTop was the previous min
}

// Equal to TopSumAddReplaceEffect, but the swap is instead from top to nonTop. Different processing though.
type TopSumSubReplaceEffect struct {
	newElem         TopKScore //Element that went to notInTop
	newElemOldScore int32     //The element's previous value when it was on top
	oldElem         TopKScore //The element that got promoted from notInTop to Top. It is the current minimum
	oldMin          TopKScore //The minimum before this change
}*/

func (score TopKScore) copy() (newScoreP *TopKScore) {
	newScoreP = new(TopKScore)
	newScoreP.Id, newScoreP.Score, newScoreP.Data = score.Id, score.Score, score.Data
	return
}

func (score TopKScore) isHigher(other *TopKScore) bool {
	if score.Score > other.Score {
		return true
	}
	return score.Score == other.Score && score.Id > other.Id
}

func (score TopKScore) isSmaller(other *TopKScore) bool {
	if score.Score < other.Score {
		return true
	}
	return score.Score == other.Score && score.Id < other.Id
}

// An element got incremented. Note that this element may not exist in the buffer.
// Note: elem.GetId() == origElem.GetId()
func (mb *minBuffer[T]) updateHigher(elem T, origElem T) {
	currHighest := mb.mins[mb.size-1]
	if isHigherScore(origElem, currHighest) { //origElem was too high, so new score also doesn't belong. Nothing to do.
		return
	}
	if isHigherScore(elem, currHighest) { //Only remove.
		for i, curr := range mb.mins[:mb.size] {
			if curr.GetId() == elem.GetId() {
				copy(mb.mins[i:], mb.mins[i+1:mb.size])
				mb.size--
				return
			}
		}
		fmt.Printf("[MinBuffer][updateHigher]Old elem %v was expected to be inside the buffer (and new elem %v was expected to not fit there), but it isn't. OrigElem: %v. Elem: %v. Buffer: %v.\n",
			origElem, elem, origElem, elem, mb.mins[:mb.size])
	}
	//Both belong to the buffer. Rare case. Old (rem) position will appear first.
	posToRemove := -1
	for i, curr := range mb.mins[:mb.size] {
		//[10, 14, 18, 22, 26, 30]. Adding: 28. Removing: 18. posToRemove = 2. Will match add on i = 5.
		//Copy to [2:] from [3:6], i.e., copy 2, 3, 4 to 3, 4, 5.
		//Result: [10, 14, 22, 26, 30, 30].
		//Write on i = 5. Result: [10, 14, 22, 26, 30, 28.] Oups!
		//Fixed now that we write on i-1.
		if curr.GetId() == elem.GetId() {
			posToRemove = i
		} else if isHigherScore(curr, elem) { //Belongs here. This always happens after.
			copy(mb.mins[posToRemove:], mb.mins[posToRemove+1:i+1]) //Copy with this position included.
			mb.mins[i-1] = elem                                     //Must write on i-1, because elem < curr!
			return
		}
	}
	//Will never reach this point.
	panic("[MinBuffer][updateHigher]Reached end of function, which is supposed to be impossible." +
		fmt.Sprintf("Old Elem %v was expected to be inside the buffer, but it isn't. OrigElem: %v. NewElem: %v. Buffer: %v.\n",
			origElem, origElem, elem, mb.mins[:mb.size]))
}

// Preferably, use updateLower/updateHigher directly, to better convey the semantics.
// This is only intended for usage during effect reversion, to better re-use existing effects.
func (mb *minBuffer[T]) update(elem T, origElem T) {
	if isHigherScore(elem, origElem) {
		mb.updateHigher(elem, origElem)
	} else {
		mb.updateLower(elem, origElem)
	}
}

// An element got decremented.
func (mb *minBuffer[T]) updateLower(elem T, origElem T) {
	currHighest := mb.mins[mb.size-1]
	if isHigherScore(elem, currHighest) { //The new score (decremented) is too high, so origScore also doesn't belong. Nothing to do.
		return
	}
	if isHigherScore(origElem, currHighest) { //Old didn't belong, but new one does. Only add.
		for i, curr := range mb.mins[:mb.size] {
			if isHigherScore(curr, elem) { //Belongs here.
				copy(mb.mins[i+1:], mb.mins[i:mb.size])
				if mb.size < len(mb.mins) {
					mb.size++
				}
				mb.mins[i] = elem
				return
			}
		}
		fmt.Printf("[MinBuffer][updateLower]New elem %v was expected to be inside the buffer (and old elem %v was expected to not be there), but it isn't! Buffer: %v.\n", elem, origElem, mb.mins[:mb.size])
	}
	//Both belong (rare-ish case). We'll find first the add position for sure (as elem < origScore)
	addPos := -1
	for i, curr := range mb.mins[:mb.size] {
		if addPos == -1 && isHigherScore(curr, elem) {
			addPos = i
			if curr.GetId() == elem.GetId() { //This can happen if the element is updated but doesn't change its position in the buffer.
				mb.mins[i] = elem
				return
			}
		} else if curr.GetId() == elem.GetId() { //This always happens after finding addPos
			copy(mb.mins[addPos+1:], mb.mins[addPos:i])
			mb.mins[addPos] = elem
			return
		}
	}
	panic("[MinBuffer][updateLower]Reached end of function, which is supposed to be impossible." +
		fmt.Sprintf("New elem %v was expected to be able to fit into the buffer, but it didn't. OrigElem: %v. NewElem: %v. Buffer: %v.\n",
			elem, origElem, elem, mb.mins[:mb.size]))
	//Will never reach this point.
}

// An element got incremented (i.e., elem < origElem)
// Note: ID on both elements must be the same.
func (mb *maxBuffer[T]) updateHigher(elem T, origElem T) {
	currLowest := mb.maxs[mb.size-1]
	if isLowerScore(elem, currLowest) { //New score is too low, so the old one wasn't here too for sure. Nothing to do.
		return
	}
	//New one belongs for sure. Find out if old one also did.
	if isLowerScore(origElem, currLowest) { //Old didn't. So it's like adding a new value.
		for i, curr := range mb.maxs[:mb.size] {
			if isLowerScore(curr, elem) { //Belongs here.
				//Size = 3. [60, 50, 40]. origElem = 35 (doesn't belong), elem = 45 (belongs)
				//i = 2. Should add here. Copy to [3:] from [2:3] (i.e., copy [2]). Result: [60, 50, 40, 40].
				//Increase size. Size = 4. OK.
				//Set position 2 to 45. Result: [60, 50, 45, 40]. All OK.
				copy(mb.maxs[i+1:], mb.maxs[i:mb.size])
				if mb.size < len(mb.maxs) {
					mb.size++
				}
				mb.maxs[i] = elem
				return
			}
		}
		fmt.Printf("[MaxBuffer][updateHigher]New elem %v was expected to be inside the buffer (and old elem %v was expected to not be there), but it isn't! Buffer: %v.\n", elem, origElem, mb.maxs[:mb.size])
	} else { //Both belong. We'll find the position of elem before the position of origScore.
		addPos := -1
		//[30, 28, 26, 24, 22, 20, 18]. Adding: 27. Removing: 22. addPos = 2. ID matches on i = 4. Copy to 3: from 2:4 (i.e., copy 2,3; write on 3,4).
		//After copy: [30, 28, 26, 26, 24, 20, 18].
		//Write 2: [30, 28, 27, 26, 24, 20, 18]. All OK.
		//[30, 28, 26, 24, 22, 20, 18]. Adding 27. Removing 26. addPos = 2. ID will never match, as it was supposed to be on the same place.
		for i, curr := range mb.maxs[:mb.size] {
			if addPos == -1 && isLowerScore(curr, elem) {
				addPos = i
				if curr.GetId() == elem.GetId() { //This can happen if the element is updated but doesn't change its position in the buffer.
					mb.maxs[i] = elem
					return
				}
			} else if curr.GetId() == elem.GetId() { //This always happens after finding the addPos.
				/*if addPos == -1 {
					fmt.Printf("[MaxBuffer][updateHigher]Old elem %v was expected to be inside the buffer, new elem %v was expected to fit there. We found the place to fit %v, but we didn't find %v! Buffer: %v.\n",
						origElem, elem, elem, origElem, mb.maxs[:mb.size])
				}*/
				copy(mb.maxs[addPos+1:], mb.maxs[addPos:i])
				mb.maxs[addPos] = elem
				return
			}
		}
	}
	panic("[MaxBuffer][updateHigher]Reached end of function, which is supposed to be impossible." +
		fmt.Sprintf("New elem %v was expected to be able to fit into the buffer, but it didn't. OrigElem: %v. NewElem: %v. Buffer: %v.\n",
			elem, origElem, elem, mb.maxs[:mb.size]))
	//Will never reach this point.
}

// An element got decremented
func (mb *maxBuffer[T]) updateLower(elem T, origElem T, crdt *TopSumCrdt) {
	currLowest := mb.maxs[mb.size-1]
	if isLowerScore(origElem, currLowest) { //Old score was too low, so new one (which is even lower) also isn't here. Nothing to do.
		return
	}
	//Orig one belongs for sure. Find out if new one also does.
	if isLowerScore(elem, currLowest) { //New doesn't. So we simply remove existing entry.
		for i, curr := range mb.maxs[:mb.size] {
			if curr.GetId() == elem.GetId() {
				copy(mb.maxs[i:], mb.maxs[i+1:mb.size])
				mb.size--
				return
			}
		}
		fmt.Printf("[MaxBuffer][updateLower]Old elem %v was expected to be inside the buffer (and new elem %v was expected to not fit there), but it isn't! Buffer: %v.\n", origElem, elem, mb.maxs[:mb.size])
	} else { //Both belong (rare). We'll find the position to remove (origElem) before the add position
		remPos := -1
		for i, curr := range mb.maxs[:mb.size] {
			//[30, 28, 26, 22, 20, 18]. Adding: 21. Removing: 28. remPos = 1. Lower score will match on 20 (i=4).
			//copy([1:], [2:5]). I.e., copy 2, 3, 4 to 1, 2, 3.
			//Result: [30, 26, 22, 20, 20, 18].
			//Write 21 to position 4. Result: [30, 26, 22, 20, 21, 18]. Oups!
			//Fixed now that we write on i-1.
			//[30, 28, 26, 22, 20, 18]. Adding: 21. Removing: 22. remPos = 3.
			//Will work ok, as 20 < 21 (next position) and then it will update i-1, correctly.
			if curr.GetId() == elem.GetId() {
				remPos = i
			} else if isLowerScore(curr, elem) {
				/*if remPos == -1 {
					fmt.Printf("[MaxBuffer][updateLower]Old elem %v was expected to be inside the buffer, new elem %v was expected to fit there. We found the place to fit %v, but we didn't find %v! Buffer: %v.\n", origElem, elem, elem, origElem, mb.maxs[:mb.size])
					break
				}*/
				copy(mb.maxs[remPos:], mb.maxs[remPos+1:i+1]) //Copy all from remPos+1 until this position, inclusive.
				mb.maxs[i-1] = elem                           //We need to write on the position before (i-1), as elem > curr!
				return
			}
		}
	}
	//Will never reach this point.
	//fmt.Sprintf("[MaxBuffer][updateLower]Elem %v was expected to be inside the buffer, but it isn't. OrigElem: %v. NewElem: %v. Buffer: %v.\n",
	//	origElem, origElem, elem, mb.maxs[:mb.size])
	origBuf := newMaxBuffer[T](cap(mb.maxs), mb.zero)
	copy(origBuf.maxs, mb.maxs)
	origBuf.size = mb.size
	crdt.findAndUpdateNotTopMax()

	panic("[MaxBuffer][updateLower]Reached end of function, which is supposed to be impossible." +
		fmt.Sprintf("Old Elem %v was expected to be inside the buffer, but it isn't. OrigElem: %v. NewElem: %v. Buffer: %v. Recalc buffer: %v. len(crdt.notInTop): %d.\n",
			origElem, origElem, elem, origBuf.maxs[:origBuf.size], mb.maxs[:mb.size], len(crdt.notInTop)))
}

func (mb *minBuffer[T]) sanityCheck() {
	if mb.size == 0 {
		panic("[MinBuffer][sanityCheck]Size is 0 during sanityCheck, this is unexpected.")
	}
	previous := mb.mins[0]
	for i := 1; i < mb.size; i++ {
		if !isHigherScore(mb.mins[i], previous) {
			panic(fmt.Sprintf("[MinBuffer][sanityCheck]Found elements out of order at pos %d. Previous: %v. Current: %v. Buffer: %v.\n", i, previous, mb.mins[i], mb.mins[:mb.size]))
		}
		previous = mb.mins[i]
	}
}

func (mb *maxBuffer[T]) sanityCheck() {
	if mb.size == 0 {
		panic("[MaxBuffer][sanityCheck]Size is 0 during sanityCheck, this is unexpected.")
	}
	previous := mb.maxs[0]
	for i := 1; i < mb.size; i++ {
		if !isLowerScore(mb.maxs[i], previous) {
			panic(fmt.Sprintf("[MaxBuffer][sanityCheck]Found elements out of order at pos %d. Previous: %v. Current: %v. Buffer: %v.\n", i, previous, mb.maxs[i], mb.maxs[:mb.size]))
		}
		previous = mb.maxs[i]
	}
}

func (mb *minBuffer[T]) sanityCheckNotPanic() (msg string, ok bool) {
	if mb.size == 0 {
		return "[MinBuffer][sanityCheckNotPanic]Size is 0 during sanityCheck, this is unexpected.", false
	}
	previous := mb.mins[0]
	for i := 1; i < mb.size; i++ {
		if !isHigherScore(mb.mins[i], previous) {
			return fmt.Sprintf("[MinBuffer][sanityCheckNotPanic]Found elements out of order at pos %d. Previous: %v. Current: %v. Buffer: %v.\n", i, previous, mb.mins[i], mb.mins[:mb.size]), false
		}
		previous = mb.mins[i]
	}
	return "", true
}

func (mb *maxBuffer[T]) sanityCheckNotPanic() (msg string, ok bool) {
	if mb.size == 0 {
		return "[MaxBuffer][sanityCheckNotPanic]Size is 0 during sanityCheck, this is unexpected.", false
	}
	previous := mb.maxs[0]
	for i := 1; i < mb.size; i++ {
		if !isLowerScore(mb.maxs[i], previous) {
			return fmt.Sprintf("[MaxBuffer][sanityCheckNotPanic]Found elements out of order at pos %d. Previous: %v. Current: %v. Buffer: %v.\n", i, previous, mb.maxs[i], mb.maxs[:mb.size]), false
		}
		previous = mb.maxs[i]
	}
	return "", true
}

func sanityCheckBuffers[T HasScoreId](minB minBuffer[T], maxB maxBuffer[T]) (msg string, ok bool) {
	if minB.size == 0 {
		return "[sanityCheckBuffers]Min buffer size is 0 during sanityCheck, this is unexpected.", false
	}
	if maxB.size == 0 {
		return "[sanityCheckBuffers]Max buffer size is 0 during sanityCheck, this is unexpected.", false
	}
	previous := minB.mins[0]
	for i := 1; i < minB.size; i++ {
		if !isHigherScore(minB.mins[i], previous) {
			return fmt.Sprintf("[MinBuffer][sanityCheckNotPanic]Found elements out of order at pos %d. Previous: %v. Current: %v. Buffer: %v.\n", i, previous, minB.mins[i], minB.mins[:minB.size]), false
		}
		previous = minB.mins[i]
	}
	if !isLowerScore(maxB.maxs[0], minB.mins[0]) { //The min in "minB" must be higher than the max in maxB.
		return fmt.Sprintf("[sanityCheckBuffers]Found min buffer with min %v that is not higher than max buffer with max %v. Min buffer: %v. Max buffer: %v.\n", minB.mins[0], maxB.maxs[0], minB.mins[:minB.size], maxB.maxs[:maxB.size]), false
	}
	previous = maxB.maxs[0]
	for i := 1; i < maxB.size; i++ {
		if !isLowerScore(maxB.maxs[i], previous) {
			return fmt.Sprintf("[MaxBuffer][sanityCheckNotPanic]Found elements out of order at pos %d. Previous: %v. Current: %v. Buffer: %v.\n", i, previous, maxB.maxs[i], maxB.maxs[:maxB.size]), false
		}
		previous = maxB.maxs[i]
	}
	return "", true
}

type MaxHeapTopKScore []TopKScore

func newMaxHeapTopKScore(size int) *MaxHeapTopKScore {
	h := make(MaxHeapTopKScore, 0, size)
	return &h
}

func (h MaxHeapTopKScore) Len() int           { return len(h) }
func (h MaxHeapTopKScore) Less(i, j int) bool { return h[i].isHigher(&h[j]) } // reversed for max-heap
func (h MaxHeapTopKScore) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MaxHeapTopKScore) Push(x any) {
	*h = append(*h, x.(TopKScore))
}

func (h *MaxHeapTopKScore) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type MinHeapTopKScore []TopKScore

func newMinHeapTopKScore(size int) *MinHeapTopKScore {
	h := make(MinHeapTopKScore, 0, size)
	return &h
}

func (h MinHeapTopKScore) Len() int           { return len(h) }
func (h MinHeapTopKScore) Less(i, j int) bool { return h[i].isSmaller(&h[j]) } // min-heap
func (h MinHeapTopKScore) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeapTopKScore) Push(x any) {
	*h = append(*h, x.(TopKScore))
}

func (h *MinHeapTopKScore) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (crdt *TopSumCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }
func (crdt *TopSumCrdt) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

// Ops
func (args TopSAdd) GetCRDTType() proto.CRDTType          { return proto.CRDTType_TOPSUM }
func (args TopSAddAll) GetCRDTType() proto.CRDTType       { return proto.CRDTType_TOPSUM }
func (args TopSSub) GetCRDTType() proto.CRDTType          { return proto.CRDTType_TOPSUM }
func (args TopSSubAll) GetCRDTType() proto.CRDTType       { return proto.CRDTType_TOPSUM }
func (args TopSInit) GetCRDTType() proto.CRDTType         { return proto.CRDTType_TOPSUM }
func (args TopSAddAndSubAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }
func (args TopSAdd) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }
func (args TopSAddAll) GetDATAType() proto.DATAType       { return proto.DATAType_DEFAULT }
func (args TopSSub) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }
func (args TopSSubAll) GetDATAType() proto.DATAType       { return proto.DATAType_DEFAULT }
func (args TopSInit) GetDATAType() proto.DATAType         { return proto.DATAType_DEFAULT }
func (args TopSAddAndSubAll) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

// Downstreams
func (args DownstreamTopSAdd) GetCRDTType() proto.CRDTType          { return proto.CRDTType_TOPSUM }
func (args DownstreamTopSAdd) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }
func (args DownstreamTopSAdd) MustReplicate() bool                  { return *args.replicate }
func (args DownstreamTopSAddAll) GetCRDTType() proto.CRDTType       { return proto.CRDTType_TOPSUM }
func (args DownstreamTopSAddAll) GetDATAType() proto.DATAType       { return proto.DATAType_DEFAULT }
func (args DownstreamTopSAddAll) MustReplicate() bool               { return len(*args.Scores) > 0 }
func (args DownstreamTopSSub) GetCRDTType() proto.CRDTType          { return proto.CRDTType_TOPSUM }
func (args DownstreamTopSSub) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }
func (args DownstreamTopSSub) MustReplicate() bool                  { return *args.replicate }
func (args DownstreamTopSSubAll) GetCRDTType() proto.CRDTType       { return proto.CRDTType_TOPSUM }
func (args DownstreamTopSSubAll) GetDATAType() proto.DATAType       { return proto.DATAType_DEFAULT }
func (args DownstreamTopSSubAll) MustReplicate() bool               { return len(*args.Scores) > 0 }
func (args DownstreamTopSAddAndSubAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }
func (args DownstreamTopSAddAndSubAll) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args DownstreamTopSAddAndSubAll) MustReplicate() bool {
	return len(*args.AddScores)+len(*args.SubScores) > 0
}
func (args TopSInit) MustReplicate() bool { return true }

// States
func (args TopSValueState) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }
func (args TopSValueState) GetREADType() proto.READType { return proto.READType_FULL }
func (args TopSValueState) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

// Reads
func (args GetTopSumNArguments) GetCRDTType() proto.CRDTType          { return proto.CRDTType_TOPSUM }
func (args GetTopSumNArguments) GetREADType() proto.READType          { return proto.READType_GET_N }
func (args GetTopSumNArguments) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }
func (args GetTopSumNArguments) HasInnerReads() bool                  { return false }
func (args GetTopSumNArguments) HasVariables() bool                   { return false }
func (args GetTopSumAboveValueArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }
func (args GetTopSumAboveValueArguments) GetREADType() proto.READType {
	return proto.READType_GET_ABOVE_VALUE
}
func (args GetTopSumAboveValueArguments) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args GetTopSumAboveValueArguments) HasInnerReads() bool         { return false }
func (args GetTopSumAboveValueArguments) HasVariables() bool          { return false }

const (
	SPECIAL_SCORE_DATA = 0xED
)

var (
	defaultTopSSize = 100 //Default number of top positions
	MIN_SCORE       = TopKScore{Id: math.MinInt32, Score: math.MinInt32}
	SPECIAL_SCORE   = TopKScore{Id: math.MinInt32, Score: math.MinInt32, Data: tools.NewByteSlicePtr([]byte{SPECIAL_SCORE_DATA})}
)

func (crdt *TopSumCrdt) Initialize(startTs clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	return crdt.InitializeWithSize(startTs, replicaID, defaultTopSSize)
}

func (crdt *TopSumCrdt) InitializeWithSize(startTs clocksi.Timestamp, replicaID uint16, size int) (newCrdt CRDT) {
	crdt = &TopSumCrdt{
		CRDTVM:        (&genericInversibleCRDT{}).initialize(crdt),
		maxElems:      size,
		elems:         make(map[int32]*TopKScore),
		notInTop:      make(map[int32]*TopKScore),
		notPropagated: make(map[int32]*TopKScore),
	}
	crdt.initializeBuffers()
	newCrdt = crdt
	return
}

func (crdt *TopSumCrdt) initializeBuffers() {
	crdt.smallestScores = newMinBuffer(tools.Max(minBufferSize, int(float64(crdt.maxElems)*MIN_BUF_FACTOR)), MIN_SCORE)
	//It's hard to assign a size to highestNotTop, as there's no limit to notInTop size.
	//We'll first use elems as a reference and then, on findAndUpdateNotTopMax(), we'll evaluate notInTop's size and grow accordingly.
	crdt.highestNotTop = newMaxBuffer(tools.Max(minBufferSize, int(float64(crdt.maxElems)*MIN_BUF_FACTOR)), MIN_SCORE)
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *TopSumCrdt) initializeFromSnapshot(startTs clocksi.Timestamp, replicaID uint16) (sameCRDT *TopSumCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *TopSumCrdt) IsBigCRDT() bool { return crdt.maxElems > 100 && len(crdt.elems) > 100 }

// Note: Also accepts TopK types of reads.
func (crdt *TopSumCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	crdt.nReads++
	if crdt.sortedElems == nil && (crdt.nUpds <= 1 || (crdt.nReads)/(crdt.nUpds+1) > 10) { //nUpds+1 to be safe on the case there's no updates yet. nUpds <= 1 to account for initial data setting (usually with TopKAddAll)
		crdt.makeSortedElems()
	}
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case GetTopSumNArguments:
		return crdt.getTopN(typedArgs.NumberEntries, updsNotYetApplied)
	case GetTopNArguments:
		return crdt.getTopN(typedArgs.NumberEntries, updsNotYetApplied)
	case GetTopSumAboveValueArguments:
		return crdt.getTopSAboveValue(typedArgs.MinValue, updsNotYetApplied)
	case GetTopKAboveValueArguments:
		return crdt.getTopSAboveValue(typedArgs.MinValue, updsNotYetApplied)
	case TopAggregateArguments:
		return crdt.getTopAggregate(typedArgs.MinValue, typedArgs.MaxValue, typedArgs.Bitmask, typedArgs.AggregateType, updsNotYetApplied)
	default:
		fmt.Printf("[TOPSUMCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *TopSumCrdt) makeSortedElems() {
	values := make([]TopKScore, len(crdt.elems))
	i := 0
	for _, elem := range crdt.elems {
		values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
		i++
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Score > values[j].Score })
	crdt.sortedElems = values
}

func (crdt *TopSumCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	//values := make([]TopKScore, len(crdt.elems))
	var values []TopKScore
	if crdt.sortedElems != nil {
		values = crdt.sortedElems
		//copy(values, crdt.sortedElems)
	} else {
		values = make([]TopKScore, len(crdt.elems))
		i := 0
		for _, elem := range crdt.elems {
			values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
			i++
		}
	}
	return TopSValueState{Scores: values}
}

// Note: in the current implementation, at most N entries are returned, even if N+1 has the same value as N.
func (crdt *TopSumCrdt) getTopN(numberEntries int32, updsNotYetApplied []UpdateArguments) (state State) {
	if crdt.sortedElems == nil {
		//crdt.sortedElems = crdt.getState(updsNotYetApplied).(TopSValueState).Scores
		//sort.Slice(crdt.sortedElems, func(i, j int) bool { return crdt.sortedElems[i].Score > crdt.sortedElems[j].Score })
		crdt.makeSortedElems()
	}
	if numberEntries >= int32(len(crdt.sortedElems)) {
		return TopSValueState{Scores: crdt.sortedElems}
	}
	return TopSValueState{Scores: crdt.sortedElems[:numberEntries]}
}

func (crdt *TopSumCrdt) getTopSAboveValue(minValue int32, updsNotYetApplied []UpdateArguments) (state State) {
	var values []TopKScore
	actuallyAdded := -1
	//Faster to do with sortedElems if it's available.
	if crdt.sortedElems != nil {
		/*for _, elem := range crdt.sortedElems {
			if elem.Score >= minValue {
				values[actuallyAdded] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
				actuallyAdded++
			} else {
				break
			}
		}*/
		if !crdt.smallestScores.hasMin() || minValue <= crdt.smallestScores.getMin().Score { //If it doesn't have min, the top is empty. So we can use this codepath.
			//values = make([]TopKScore, len(crdt.elems))
			//copy(values, crdt.sortedElems)
			values = crdt.sortedElems
		} else if len(crdt.sortedElems) > 200 { //Attempt to find the end position with binary search.
			//Binary search.
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
			//values = make([]TopKScore, left)
			//copy(values, crdt.sortedElems[:left])
			values = crdt.sortedElems[:left]
		} else { //Just iterate and find the end position manually.
			//values = make([]TopKScore, len(crdt.elems))
			for i, elem := range crdt.sortedElems {
				if elem.Score >= minValue {
					//values[actuallyAdded] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
					//values[i] = elem //This will copy as its a value type.
				} else {
					actuallyAdded = i
					break
				}
			}
			if actuallyAdded == -1 {
				actuallyAdded = len(crdt.elems)
			}
			values = crdt.sortedElems[:actuallyAdded]
		}
		return TopSValueState{Scores: values}
	} else {
		values = make([]TopKScore, len(crdt.elems))
		//Must go through all elems
		for _, elem := range crdt.elems {
			if elem.Score >= minValue {
				values[actuallyAdded] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
				actuallyAdded++
			}
		}
	}

	return TopSValueState{Scores: values[:actuallyAdded]}

}

func (crdt *TopSumCrdt) getTopAggregate(minValue, maxValue, bitmask int32, aggrType AggregateType, updsNotYetApplied []UpdateArguments) (state State) {
	//fmt.Printf("[TopSumCRDT]GetTopAggregate with min %d, max %d, bitmask %x, aggrType %d, topEntries %d.\n", minValue, maxValue, bitmask, aggrType, len(crdt.elems))
	if len(crdt.elems) == 0 {
		return getAggregateState(aggrType, 0)
	}
	minScore := crdt.smallestScores.getMin().Score
	if aggrType == M_MIN && minScore > minValue { //This is already known.
		return getAggregateState(aggrType, int64(minScore))
	}
	if crdt.sortedElems != nil {
		if aggrType == M_MAX && crdt.sortedElems[0].Score < maxValue { //This is already known.
			return getAggregateState(aggrType, int64(crdt.sortedElems[0].Score))
		}
		return aggrStrategyChooserSortedElems(crdt.sortedElems, crdt.sortedElems[0].Score, minScore, minValue, maxValue, bitmask, aggrType)
	} else {
		return aggrStrategyChooserTopKMap(crdt.elems, minScore, minValue, maxValue, bitmask, aggrType)
	}
}

func (crdt *TopSumCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	//return crdt.getTopSAddDownstreamArgs(args.(TopSAdd))
	switch typedArgs := args.(type) {
	case TopSAdd:
		return crdt.getTopSAddDownstreamArgs(typedArgs)
	case TopSSub:
		return crdt.getTopSSubDownstreamArgs(typedArgs)
	case TopSAddAll:
		return crdt.getTopSAddAllDownstreamArgs(typedArgs)
	case TopSSubAll:
		return crdt.getTopSSubAllDownstreamArgs(typedArgs)
	case TopSAddAndSubAll:
		return crdt.getTopSAddAndSubAllDownstreamArgs(typedArgs)
	case TopSInit:
		return crdt.getTopSInitDownstreamArgs(typedArgs)
	case MultiUpd:
		multiDowns := make(MultiUpd, len(typedArgs))
		for i, innerUpd := range typedArgs {
			multiDowns[i] = crdt.Update(innerUpd)
		}
		return multiDowns
	default:
		fmt.Printf("[TopSum][Update]Unknown update type: %v (%T)\n", args, args)
	}
	return nil
}

func (crdt *TopSumCrdt) getTopSInitDownstreamArgs(initOp TopSInit) (args TopSInit) {
	if len(crdt.elems) == 0 { //Set nElems immediately if it's the first op, in order for upcoming Update() to make correct decisions. This does not affect correctness.
		crdt.maxElems = int(initOp)
	}
	return initOp
}

func (crdt *TopSumCrdt) getTopSAddDownstreamArgs(addOp TopSAdd) (downstreamArgs DownstreamArguments) {
	if addOp.Data == nil {
		addOp.Data = emptyData
	}
	return DownstreamTopSAdd{TopKScore: addOp.TopKScore, replicate: new(bool), srcReplicaID: shared.ReplicaID} //new(bool): false
}

func (crdt *TopSumCrdt) getTopSAddAllDownstreamArgs(addOp TopSAddAll) (downstreamArgs DownstreamArguments) {
	for i, score := range addOp.Scores {
		if score.Data == nil {
			score.Data = emptyData
			addOp.Scores[i] = score
		}
	}
	return DownstreamTopSAddAll{Scores: &addOp.Scores, srcReplicaID: shared.ReplicaID}
}

func (crdt *TopSumCrdt) getTopSSubDownstreamArgs(subOp TopSSub) (downstreamArgs DownstreamArguments) {
	if subOp.Data == nil {
		subOp.Data = emptyData
	}
	return DownstreamTopSSub{TopKScore: subOp.TopKScore, replicate: new(bool)}
}

func (crdt *TopSumCrdt) getTopSSubAllDownstreamArgs(subOp TopSSubAll) (downstreamArgs DownstreamArguments) {
	for i, score := range subOp.Scores {
		if score.Data == nil {
			score.Data = emptyData
			subOp.Scores[i] = score
		}
	}
	return DownstreamTopSSubAll{Scores: &subOp.Scores}
}

func (crdt *TopSumCrdt) getTopSAddAndSubAllDownstreamArgs(addSubOp TopSAddAndSubAll) (downstreamArgs DownstreamArguments) {
	for i, score := range addSubOp.AddScores {
		if score.Data == nil {
			score.Data = emptyData
			addSubOp.AddScores[i] = score
		}
	}
	for i, score := range addSubOp.SubScores {
		if score.Data == nil {
			score.Data = emptyData
			addSubOp.SubScores[i] = score
		}
	}
	return DownstreamTopSAddAndSubAll{AddScores: &addSubOp.AddScores, SubScores: &addSubOp.SubScores, srcReplicaID: shared.ReplicaID}
}

func (crdt *TopSumCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		//fmt.Printf("[TOPSUM]Original args type: %T.\n", downstreamArgs)
		var otherDown MultiUpd
		for _, innerDown := range multiUpd {
			newDown := crdt.Downstream(updTs, innerDown.(DownstreamArguments))
			if newDown != nil {
				otherDown = append(otherDown, newDown)
			}
		}
		if len(otherDown) == 0 {
			return nil
		}
		return otherDown
	}
	effect, otherDownstreamArgs := crdt.applyDownstream(downstreamArgs)
	//effect, _ := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(updTs, downstreamArgs, effect)
	return otherDownstreamArgs
}

func (crdt *TopSumCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect Effect, otherDownstreamArgs DownstreamArguments) {
	switch typedArgs := downstreamArgs.(type) {
	case DownstreamTopSAdd:
		return crdt.applyTopSAddDownstreamArgs(typedArgs)
	case DownstreamTopSSub:
		return crdt.applyTopSSubDownstreamArgs(typedArgs), nil
	case DownstreamTopSAddAll:
		return crdt.applyTopSAddAllDownstreamArgs(typedArgs)
	case DownstreamTopSSubAll:
		return crdt.applyTopSSubAllDownstreamArgs(typedArgs), nil
	case DownstreamTopSAddAndSubAll:
		addOp, subOp := DownstreamTopSAddAll{Scores: typedArgs.AddScores, srcReplicaID: typedArgs.srcReplicaID}, DownstreamTopSSubAll{Scores: typedArgs.SubScores}
		//Note: Sub never generates otherDownstreamArgs, only Add does.
		/*if (*addOp.Scores)[0].Score < 0 {
			fmt.Printf("[TopSum][Downstream]Error: received DownstreamTopSAddAndSubAll whose first score of AddScores is negative. AddOp: %+v. SubOp: %+v.\n", *addOp.Scores, *subOp.Scores)
		}*/
		addEffect, addOtherDowns := crdt.applyTopSAddAllDownstreamArgs(addOp)
		remEffect := crdt.applyTopSSubAllDownstreamArgs(subOp)
		*typedArgs.AddScores, *typedArgs.SubScores = *addOp.Scores, *subOp.Scores //Updating scores to be downstreamed.
		return TopSumMultiEffect{addEffect, remEffect}, addOtherDowns
	case TopSInit:
		return crdt.applyInit(uint32(typedArgs))
	default:
		fmt.Printf("[TopSum][Downstream]Unsupported downstream type %v (%T)\n", downstreamArgs, downstreamArgs)
	}
	return nil, nil
}

func (crdt *TopSumCrdt) applyInit(size uint32) (effect Effect, otherDownstreamArgs DownstreamArguments) {
	if int(size) > crdt.maxElems*10 && len(crdt.elems) == 0 {
		crdt.elems = make(map[int32]*TopKScore, int(size)) //Resize.
	}
	crdt.maxElems, effect = int(size), NoEffect{}
	crdt.initializeBuffers()
	if len(crdt.elems) > 0 { //This is outside Init's intended usage.
		crdt.findAndUpdateMin()
		if len(crdt.notInTop) > 0 {
			crdt.findAndUpdateNotTopMax()
		}
	}
	//fmt.Println("[TopSum]Max top size set to", crdt.maxElems)
	return
}

func (crdt *TopSumCrdt) applyTopSAddDownstreamArgs(op DownstreamTopSAdd) (effect Effect, otherDownstreamArgs DownstreamArguments) {
	/*if op.Score <= 0 {
		fmt.Printf("[TopSum][applyTopSAddDownstreamArgs]Error: score received is negative or 0. Op: %+v.\n", op)
		panic(0)
	}*/
	ourReplicaID := shared.ReplicaID
	effect = NoEffect{}
	//Case 1: no elements yet
	if len(crdt.elems) == 0 {
		crdt.smallestScores.add(op.TopKScore)
		crdt.elems[op.Id] = op.TopKScore.copy()
		*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
		return TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32}, nil
	}

	//Case 2: increase to element on top, thus gets propagated.
	entry, has := crdt.elems[op.Id]
	if has {
		oldEntry := *entry
		//Increase
		entry.Score += op.Score
		effect = TopSumAddEffect{newScore: *entry, oldScore: oldEntry.Score}
		*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
		crdt.smallestScores.updateHigher(*entry, oldEntry)
		if !crdt.smallestScores.hasMin() {
			crdt.findAndUpdateMin()
		}
		/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
		if !ok {
			panic(fmt.Sprintf("Old score: %v. New score: %v. Error: %s.\n", oldEntry, *entry, err))
		}*/
		/*if crdt.smallestScore.Id == entry.Id {
			//This was the min, which is now updated
			crdt.findAndUpdateMin()
		}*/
		return effect, nil
	} else if len(crdt.elems) < crdt.maxElems {
		//Case 3: not enough elements yet, and this element doesn't exist yet.
		copy := op.TopKScore.copy()
		crdt.elems[op.Id] = copy
		//check if it should now be min
		crdt.smallestScores.addIfInBetween(op.TopKScore, len(crdt.elems))
		/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
		if !ok {
			panic(fmt.Sprintf("New score: %v. Error: %s.\n", *copy, err))
		}*/
		effect = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
		/*if crdt.smallestScore.isHigher(copy) {
			effectValue = TopSumAddEffect{newScore: op.TopKScore,
				oldMin: *crdt.smallestScore, oldScore: math.MinInt32}
			crdt.smallestScore = copy
		} else {
			effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
		}*/
		*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
		return effect, nil
	}

	//From now on, it definitely isn't on top and top is full.
	entry, has = crdt.notInTop[op.Id]
	nonPropEntry, nonPropHas := crdt.notPropagated[op.Id]
	min := crdt.smallestScores.getMin()
	if has {
		//Increase
		//oldScore := entry.Score
		oldScore := TopKScore{Id: entry.Id, Score: entry.Score}
		entry.Score += op.Score
		//Case 4: wasn't on top, but now it will be.
		if entry.isHigher(&min) {
			crdt.nUpds++
			effect = TopSumAddReplaceEffect{newElem: *entry, newElemOldScore: oldScore.Score, oldElem: min}
			crdt.notInTop[min.Id] = &min
			delete(crdt.elems, min.Id)
			delete(crdt.notInTop, entry.Id)
			crdt.elems[entry.Id] = entry
			crdt.highestNotTop.remove(oldScore)
			crdt.highestNotTop.addMax(min)
			crdt.smallestScores.removeMin()
			crdt.smallestScores.addIfInBetween(*entry, len(crdt.elems))
			if !crdt.smallestScores.hasMin() {
				crdt.findAndUpdateMin()
			}
			/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
			if !ok {
				panic(fmt.Sprintf("Old score: %v. New score: %v. Old Min: %v. New Min: %v. Error: %s.\n", oldScore, *entry, min, crdt.smallestScores.getMin(), err))
			}*/
			//crdt.findAndUpdateMin()

			if nonPropHas {
				//Include those on op
				op.Score += nonPropEntry.Score
				if len(*op.Data) == 0 {
					op.Data = nonPropEntry.Data
				}
				delete(crdt.notPropagated, op.Id)
				if op.srcReplicaID != ourReplicaID && nonPropEntry.Score != 0 { //It's possible that multiple non-propagated adds and subs lead to a total of 0. In that case, nothing to propagate.
					//Remote operation, so we must force the propagation of nonPropEntry.Score
					if nonPropEntry.Score < 0 { //It may be negative, as we sometimes don't propagate decrements.
						return effect, DownstreamTopSSub{TopKScore: TopKScore{Id: op.Id, Score: -nonPropEntry.Score, Data: nonPropEntry.Data},
							replicate: new(bool)}
					} //else, if the score is positive:
					return effect, DownstreamTopSAdd{TopKScore: TopKScore{Id: op.Id, Score: nonPropEntry.Score, Data: nonPropEntry.Data},
						replicate: new(bool), srcReplicaID: ourReplicaID}
				}
			}
			//If nonPropHas = false, then this op by itself is enough to put on top (and thus, must be replicated)
			*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
			return effect, nil
		}
		//From here on, we know it is in notTop, and it won't go to the top. So we must update highestNotTop.
		crdt.highestNotTop.updateHigher(*entry, oldScore) //Note: this will never make highestNotTop smaller.
		/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
		if !ok {
			panic(fmt.Sprintf("Old score: %v. New score: %v. Error: %s.\n", oldScore, *entry, err))
		}*/
		//Case 5: won't go to top, but the element exists in notInTop. Processing differs depending if this is a local or a remote operation
		if op.srcReplicaID != ourReplicaID && nonPropHas {
			effect = TopSumAddNotTopEffect{newScore: *entry, oldScore: oldScore.Score}
			//Apply rule for existing nonProp. Return right away.
			if crdt.shouldReplicate(entry.Score, nonPropEntry.Score, min.Score) {
				delete(crdt.notPropagated, entry.Id)
				if nonPropEntry.Score < 0 { //It may be negative, as we sometimes don't propagate decrements.
					return effect, DownstreamTopSSub{TopKScore: TopKScore{Id: op.Id, Score: -nonPropEntry.Score, Data: nonPropEntry.Data},
						replicate: new(bool)}
				}
				return effect, DownstreamTopSAdd{TopKScore: TopKScore{Id: op.Id, Score: nonPropEntry.Score, Data: nonPropEntry.Data},
					replicate: new(bool), srcReplicaID: ourReplicaID}
			}
			return effect, nil
		}
		//Apply rule to check if it should be replicated.
		if nonPropEntry == nil {
			nonPropEntry = &op.TopKScore
			crdt.notPropagated[entry.Id] = nonPropEntry
		} else {
			nonPropEntry.Score += op.Score
		}
		if crdt.shouldReplicate(entry.Score, nonPropEntry.Score, min.Score) {
			op.Score = nonPropEntry.Score //nonPropEntry already includes op.Score
			if len(*op.Data) == 0 {
				op.Data = nonPropEntry.Data
			}
			delete(crdt.notPropagated, entry.Id)
			*op.replicate = true
			/*if op.Score < 0 {
				panic(fmt.Sprintf("[TopSum][add]Unexpected negative score, after adding notProp. Entry: %+v.\n", op))
			}*/
		}
		return TopSumAddNotTopEffect{newScore: *entry, oldScore: oldScore.Score}, nil
	}

	newEntry := op.TopKScore.copy()
	//Case 6: new elem that will be on top
	//In this case, it's not in elems or notOnTop. So, new entry.
	if op.TopKScore.isHigher(&min) {
		effect = TopSumAddReplaceEffect{newElem: op.TopKScore, newElemOldScore: math.MinInt32}
		crdt.notInTop[min.Id] = &min
		delete(crdt.elems, min.Id)
		crdt.elems[newEntry.Id] = newEntry
		crdt.highestNotTop.addMax(min)
		crdt.smallestScores.removeMin()
		crdt.smallestScores.addIfInBetween(*newEntry, len(crdt.elems))
		if !crdt.smallestScores.hasMin() {
			crdt.findAndUpdateMin()
		}
		/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
		if !ok {
			panic(fmt.Sprintf("New score: %v. Old Min: %v. New Min: %v. Error: %s.\n", *entry, min, crdt.smallestScores.getMin(), err))
		}*/
		//crdt.findAndUpdateMin()
		*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
		return effect, nil
	}

	crdt.notInTop[newEntry.Id] = newEntry
	crdt.highestNotTop.addIfInBetween(op.TopKScore, len(crdt.notInTop))
	/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
	if !ok {
		panic(fmt.Sprintf("New score: %v. Error: %s.\n", *newEntry, err))
	}*/
	//Case 7: new elem that isn't on top. It may still, however, need to be replicated.
	if crdt.shouldReplicate(0, op.Score, min.Score) {
		*op.replicate = true
		return TopSumAddNotTopEffect{newScore: op.TopKScore, oldScore: math.MinInt32}, nil
	}
	//Case 8: new elem, and add is too small to be worth replicating.
	crdt.notPropagated[newEntry.Id] = newEntry.copy()
	return TopSumAddNotTopEffect{newScore: op.TopKScore, oldScore: math.MinInt32}, nil
}

func (crdt *TopSumCrdt) resizeTopSumIfBigUpdate(nPossibleUpds int) {
	if nPossibleUpds > 100*(len(crdt.notInTop)+crdt.maxElems) { //In this case, notInTop would suffer multiple resizes. We will pre-allocate.
		//This optimization is mostly aimed for when initializing Tops with a lot of data, e.g., for views.
		//I may need to consider in the future also pre-allocating notPropagated.
		newSize := len(crdt.notInTop) + nPossibleUpds - (crdt.maxElems - len(crdt.elems))
		newNotInTop := make(map[int32]*TopKScore, newSize)
		tools.MapCopyFromTo(crdt.notInTop, newNotInTop)
		crdt.notInTop = newNotInTop
	}
}

func (crdt *TopSumCrdt) applyTopSAddAllDownstreamArgs(op DownstreamTopSAddAll) (effect Effect, otherDownstreamArgs DownstreamArguments) {
	scores := *op.Scores
	if len(scores) == 0 {
		var effectValue Effect = NoEffect{}
		return &effectValue, nil
	}
	effects := make(TopSumMultiEffect, len(scores))
	downScores := make([]TopKScore, len(scores))
	var otherDownScores []TopKScore
	ourReplicaID := shared.ReplicaID
	if ourReplicaID != op.srcReplicaID {
		//May need to generate some elements for otherDownstreamArgs
		otherDownScores = make([]TopKScore, 0, 10) //Will just use append
	}
	downI := 0
	var currScore TopKScore
	i := 0
	changedTop := false

	crdt.resizeTopSumIfBigUpdate(len(*op.Scores))

	//Case 1: no elements yet
	if len(crdt.elems) == 0 {
		currScore, changedTop = scores[0], true
		i++
		crdt.smallestScores.add(currScore)
		crdt.elems[currScore.Id] = currScore.copy()
		downScores[downI], downI, changedTop = currScore, downI+1, true
		effects[0] = TopSumAddEffect{newScore: currScore, oldScore: math.MinInt32}
	}
	var entry, nonPropEntry, copy *TopKScore
	var has, nonPropHas bool
	atLeastOneNegDown := false //Due to notPropagated, downScores may end up with negative entries. Should be rare though.
	for ; i < len(scores); i++ {
		currScore = scores[i]
		entry, has = crdt.elems[currScore.Id]
		if has {
			/*if currScore.Score < 0 {
				panic(fmt.Sprintf("[TopSum][applyTopSAddAll]Found negative score in addAll downstream! Index: %d. Score: %v. Op replicaID: %d. My replicaID: %d.\n", i, currScore, op.srcReplicaID, ourReplicaID))
			}*/
			oldEntry := *entry
			//Case 2: increase to element on top
			entry.Score += currScore.Score
			effects[i] = TopSumAddEffect{newScore: *entry, oldScore: oldEntry.Score}
			crdt.smallestScores.updateHigher(*entry, oldEntry)
			if !crdt.smallestScores.hasMin() {
				crdt.findAndUpdateMin()
			}
			/*if crdt.smallestScore.Id == entry.Id {
				//This was the min, which is now updated
				crdt.findAndUpdateMin()
			}*/
			downScores[downI], downI, changedTop = currScore, downI+1, true
		} else if len(crdt.elems) < crdt.maxElems {
			//Case 3: not enough elements yet, and this element doesn't exist yet.
			copy = currScore.copy()
			crdt.elems[copy.Id] = copy
			//check if it should now be min
			crdt.smallestScores.addIfInBetween(currScore, len(crdt.elems))
			effects[i] = TopSumAddEffect{newScore: currScore, oldScore: math.MinInt32}
			/*if crdt.smallestScore.isHigher(copy) {
				crdt.smallestScore = copy
			}*/
			downScores[downI], downI, changedTop = currScore, downI+1, true
		} else {
			//Not on top for sure & top is full
			entry, has = crdt.notInTop[currScore.Id]
			nonPropEntry, nonPropHas = crdt.notPropagated[currScore.Id]
			min := crdt.smallestScores.getMin()
			if has {
				/*if currScore.Score < 0 {
					panic(fmt.Sprintf("[TopSum][applyTopSAddAll]Found negative score in addAll downstream! Index: %d. Score: %v. Op replicaID: %d. My replicaID: %d.\n", i, currScore, op.srcReplicaID, ourReplicaID))
				}*/
				oldEntry := *entry
				entry.Score += currScore.Score
				//Case 4: wasn't on top, but now it will be
				//if entry.isHigher(crdt.smallestScores.getMin()) {
				if entry.isHigher(&min) {
					delete(crdt.elems, min.Id)
					delete(crdt.notInTop, entry.Id)
					crdt.elems[entry.Id] = entry
					crdt.notInTop[min.Id] = &min
					effects[i] = TopSumAddReplaceEffect{newElem: *entry, newElemOldScore: oldEntry.Score, oldElem: min}
					crdt.highestNotTop.remove(oldEntry)
					crdt.highestNotTop.addMax(min)
					crdt.smallestScores.removeMin()
					crdt.smallestScores.addIfInBetween(*entry, len(crdt.elems))
					if !crdt.smallestScores.hasMin() {
						crdt.findAndUpdateMin()
					}
					//crdt.findAndUpdateMin()
					if nonPropHas {
						//Include those on op
						currScore.Score += nonPropEntry.Score
						if len(*currScore.Data) == 0 {
							currScore.Data = nonPropEntry.Data
						}
						delete(crdt.notPropagated, currScore.Id)
						if op.srcReplicaID != ourReplicaID {
							//Remote operation, so we must force the propagation of nonPropEntry.Score
							otherDownScores = append(otherDownScores, TopKScore{Id: currScore.Id, Score: nonPropEntry.Score, Data: nonPropEntry.Data})
						} else if currScore.Score < 0 { //Score to propagate is now negative.
							atLeastOneNegDown = true
						}
					}
					downScores[downI], downI, changedTop = currScore, downI+1, true //Element went to the top: must definitely replicate this add, if the operation is local.

					//Case 5 (next): still not on top. Processing differs depending if this is a local or a remote operation
				} else if op.srcReplicaID != ourReplicaID && nonPropEntry != nil { //Remote operation, so we may have to propagate nonPropEntry.Score
					effects[i] = TopSumAddNotTopEffect{newScore: *entry, oldScore: oldEntry.Score}
					crdt.highestNotTop.updateHigher(*entry, oldEntry) //Note: this will never make highestNotTop smaller.
					if crdt.shouldReplicate(entry.Score, nonPropEntry.Score, min.Score) {
						otherDownScores = append(otherDownScores, TopKScore{Id: currScore.Id, Score: nonPropEntry.Score, Data: nonPropEntry.Data})
					}
				} else { //Local operation, so we include the nonPropEntry in the current score, if we decide to replicate.
					crdt.highestNotTop.updateHigher(*entry, oldEntry) //Note: this will never make highestNotTop smaller.
					//Apply rule to check if it should be replicated
					if nonPropEntry == nil {
						nonPropEntry = currScore.copy()
						crdt.notPropagated[entry.Id] = nonPropEntry
					} else {
						nonPropEntry.Score += currScore.Score
					}
					if crdt.shouldReplicate(entry.Score, nonPropEntry.Score, min.Score) {
						currScore.Score = nonPropEntry.Score //nonPropEntry already includes currScore.Score
						if len(*currScore.Data) == 0 {
							currScore.Data = nonPropEntry.Data
						}
						delete(crdt.notPropagated, entry.Id)
						downScores[downI], downI = currScore, downI+1 //Top didn't change here, so no need to invalidate cache.
						if currScore.Score < 0 {
							atLeastOneNegDown = true
						}
					}
					effects[i] = TopSumAddNotTopEffect{newScore: *entry, oldScore: entry.Score - currScore.Score}
				}
			} else {
				copy = currScore.copy()
				//Element doesn't exist in either top or notInTop.
				//Case 6: new elem that will be on top
				if currScore.isHigher(&min) {
					effects[i] = TopSumAddReplaceEffect{newElem: currScore, newElemOldScore: math.MinInt32}
					delete(crdt.elems, min.Id)
					crdt.elems[copy.Id] = copy
					crdt.notInTop[min.Id] = &min
					crdt.smallestScores.removeMin()
					crdt.smallestScores.addIfInBetween(*copy, len(crdt.elems))
					if !crdt.smallestScores.hasMin() {
						crdt.findAndUpdateMin()
					}
					crdt.highestNotTop.addMax(min)
					//crdt.findAndUpdateMin()
					downScores[downI], downI, changedTop = currScore, downI+1, true
				} else if crdt.shouldReplicate(0, currScore.Score, min.Score) {
					//Case 7: new elem that isn't on top but needs to be replicated
					effects[i] = TopSumAddNotTopEffect{newScore: currScore, oldScore: math.MinInt32}
					crdt.notInTop[copy.Id] = copy
					downScores[downI], downI = currScore, downI+1
					crdt.highestNotTop.addIfInBetween(currScore, len(crdt.notInTop))
				} else {
					//Case 8: new elem that isn't top and it's not high enough to be replicated
					effects[i] = TopSumAddNotTopEffect{newScore: currScore, oldScore: math.MinInt32}
					crdt.notInTop[copy.Id], crdt.notPropagated[copy.Id] = copy, copy.copy()
					crdt.highestNotTop.addIfInBetween(currScore, len(crdt.notInTop)) //Still relevant in case notInTop is still small.
				}
			}
		}
	}
	if downI > 0 && op.srcReplicaID == ourReplicaID { //If it's a remote operation, we don't replicate, so no need to update downScores.
		downScores = downScores[:downI]
		if atLeastOneNegDown { //Note: this will never be true if all negative scores belong to new IDs, as that's safe to replicate with an add.
			//Idea/trick: sort the slice as if we're preparing for DownstreamTopSAddAndSubAll. Then, at the end, add a special entry to signal that there's negative scores.
			//The score of the special entry will contain the number of positive indexes.
			nPositive := sortTopScoreEntriesPosNeg(downScores)
			if nPositive == len(downScores) { //All positive, nothing further to do. This should never happen.

			} else { //All negative or some negative is the same processing. We need to put the special entry at the end.
				markerScore := TopKScore{Id: SPECIAL_SCORE.Id, Data: SPECIAL_SCORE.Data, Score: int32(nPositive)}
				downScores = append(downScores, markerScore)
			}
			//fmt.Printf("[TopSum][applyTopSAddAllDownstreamArgs]At least one negative detected. NPositive, len: %d, %d. Last entry score: %v.\n", nPositive, len(downScores), downScores[len(downScores)-1])
		}
	}
	if changedTop {
		crdt.nUpds++
		crdt.sortedElems = nil
	}
	if len(*op.Scores) >= 1000 && len(crdt.notInTop) > 500 { //Likely an initialization operation: adjust buffers size if appropriate. Values are somewhat arbitrary.
		if crdt.resizeNotTopMaxIfNeeded() {
			crdt.findAndUpdateNotTopMax()
		}
	}
	nOpElems := len(*op.Scores)
	*op.Scores = downScores
	effect = effects
	if len(otherDownScores) > 0 {
		//fmt.Println("[TOPSUM]AddAll is returning extra downstream.")
		//We may have both negatives and positives here, but DownstreamTopSAddAll only supports positives.
		//We'll check for negatives and, if those are found, we'll issue a DownstreamTopSAddAndSubAll, and rearrange the scores accordingly.
		nPositive := sortTopScoreEntriesPosNeg(otherDownScores)
		//fmt.Printf("[TOPSUM]Add is returning extra downstream. NPositive, len: %d, %d. Last entry score: %v.\n", nPositive, len(otherDownScores), otherDownScores[len(otherDownScores)-1])
		if nPositive == len(otherDownScores) {
			return effect, DownstreamTopSAddAll{Scores: &otherDownScores, srcReplicaID: ourReplicaID}
		} else if nPositive == 0 {
			return effect, DownstreamTopSSubAll{Scores: &otherDownScores}
		} else {
			addSlice, subSlice := otherDownScores[:nPositive], otherDownScores[nPositive:]
			return effect, DownstreamTopSAddAndSubAll{AddScores: &addSlice, SubScores: &subSlice, srcReplicaID: ourReplicaID}
		}
	}

	if crdt.sortedElems == nil && nOpElems >= 100 { //Likely a initialization operation. We'll make the sortedElems now to avoid the overhead when reading.
		crdt.makeSortedElems()
	}
	//crdt.findAndUpdateMin()
	/*oldMaxBuf := crdt.highestNotTop.copy()
	crdt.findAndUpdateNotTopMax()
	for i := 0; i < oldMaxBuf.Len(); i++ {
		oldElem, newElem := oldMaxBuf.maxs[i], crdt.highestNotTop.maxs[i]
		if oldElem.Id != newElem.Id || oldElem.Score != newElem.Score {
			fmt.Printf("[TopSum][applyTopSAddAllDownstreamArgs]HighestNotTop buffer before and after recalc is different at pos %d. In original: %v. In recalc: %v. Orig buf: %v. Recalc buf: %v\n",
				i, oldElem, newElem, oldMaxBuf.maxs[:oldMaxBuf.size], crdt.highestNotTop.maxs[:crdt.highestNotTop.size])
			break
		}
	}
	crdt.highestNotTop.size = oldMaxBuf.size*/
	/*crdt.highestNotTop.sanityCheck()
	crdt.smallestScores.sanityCheck()
	msg, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
	if !ok {
		panic(msg)
	}*/
	//fmt.Printf("[TOPSUM][Inc]Top, notTop sizes: %d, %d. nMin, notTopMax calcs: %d, %d. nMin, notTopMax buffer sizes: %d, %d. nSmallestScores, nHighestScores: %d, %d.\n",
	//	len(crdt.elems), len(crdt.notInTop), crdt.nMin, crdt.nMaxNotTop, crdt.smallestScores.Cap(), crdt.highestNotTop.Cap(), crdt.smallestScores.size, crdt.highestNotTop.size)
	return effect, nil
}

func (crdt *TopSumCrdt) applyTopSSubDownstreamArgs(op DownstreamTopSSub) (effect Effect) {
	//Note: the score in the operation is always negative (hence why += is used)
	effect = NoEffect{}
	/*if op.Score <= 0 {
		fmt.Printf("[TopSum][applyTopSSubDownstreamArgs]Error: score received is negative or 0. Op: %+v.\n", op)
		panic(0)
	}*/
	/*crdt.highestNotTop.sanityCheck()
	crdt.smallestScores.sanityCheck()
	msg, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
	if !ok {
		panic(msg)
	}*/
	//Case 1: no elements yet
	if len(crdt.elems) == 0 {
		negScore := TopKScore{Id: op.Id, Score: -op.Score, Data: op.Data}
		crdt.smallestScores.add(negScore)
		crdt.elems[op.Id] = &negScore
		*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
		//new top element
		return TopSumAddEffect{newScore: negScore, oldScore: math.MinInt32}
	}

	//Case 2: decrease to element in top, thus it must be propagated
	entry, has := crdt.elems[op.Id]
	if has {
		oldScore, minScore := *entry, crdt.smallestScores.getMin()
		entry.Score -= op.Score
		if minScore.isHigher(entry) {
			if len(crdt.notInTop) == 0 || crdt.highestNotTop.getMax().isLowerScore(*entry) {
				effect = TopSumAddEffect{newScore: *entry, oldScore: oldScore.Score}
				//Special case, no move but it becomes the smallest score
				crdt.smallestScores.updateLower(*entry, oldScore)
				/*err, ok := crdt.smallestScores.sanityCheckNotPanic()
				if !ok {
					panic(fmt.Sprintf("Old score: %v. New score: %v. Min: %v. Error: %s.\n", oldScore, *entry, minScore, err))
				}*/
				/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
				if !ok {
					panic(fmt.Sprintf("Old score: %v. New score: %v. Min: %v. Error: %s.\n", oldScore, *entry, minScore, err))
				}*/
			} else {
				//Move to notTop, find new min, add that min to top.
				newMin := crdt.highestNotTop.getMax()
				crdt.highestNotTop.removeMax()
				crdt.highestNotTop.addIfInBetween(*entry, len(crdt.notInTop))
				crdt.smallestScores.remove(oldScore)
				crdt.smallestScores.addMin(newMin)
				/*err, ok := crdt.smallestScores.sanityCheckNotPanic()
				if !ok {
					panic(fmt.Sprintf("Old score: %v. New score: %v. Old Min: %v. New Min: %v. Error: %s.\n", oldScore, *entry, minScore, newMin, err))
				}*/
				//Searching for new min in nonTop; this min will be added to elems
				/*newMin := &TopKScore{Id: math.MinInt32, Score: math.MinInt32}
				for _, elem := range crdt.notInTop {
					if elem.isHigher(newMin) {
						newMin = elem
					}
				}*/
				delete(crdt.elems, op.Id)
				delete(crdt.notInTop, newMin.Id)
				crdt.notInTop[op.Id] = entry
				crdt.elems[newMin.Id] = &newMin
				//This call must be after we update crdt.notInTop, to prevent re-adding an element that was supposed to be removed.
				if !crdt.highestNotTop.hasMax() {
					crdt.findAndUpdateNotTopMax()
				}
				/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
				if !ok {
					panic(fmt.Sprintf("Old score: %v. New score: %v. Old Min: %v. New Min: %v. Error: %s.\n", oldScore, *entry, minScore, newMin, err))
				}*/
				//oldMin := crdt.smallestScore
				//crdt.smallestScore = newMin
				//replace
				effect = TopSumSubReplaceEffect{newElem: *entry, oldElem: newMin, newElemOldScore: oldScore.Score}
			}
		} else { //Stays on top.
			effect = TopSumAddEffect{newScore: *entry, oldScore: oldScore.Score}
			crdt.smallestScores.updateLower(*entry, oldScore) //Now it may belong to the min, or just need its position updated.
			/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
			if !ok {
				panic(fmt.Sprintf("Old score: %v. New score: %v. Error: %s.\n", oldScore, *entry, err))
			}*/
		}
		//Nothing do on else (element stays in top)
		*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
		return effect
	} else if len(crdt.elems) < crdt.maxElems {
		//Case 3: has space on top, and element doesn't yet exist
		copy := TopKScore{Id: op.Id, Score: -op.Score, Data: op.Data}
		crdt.elems[op.Id] = &copy
		crdt.smallestScores.addIfInBetween(copy, len(crdt.elems))
		/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
		if !ok {
			panic(fmt.Sprintf("New score: %v. Error: %s.\n", copy, err))
		}*/
		effect = TopSumAddEffect{newScore: copy, oldScore: math.MinInt32}
		/*if crdt.smallestScore.isHigher(copy) {
			effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32, oldMin: *crdt.smallestScore}
			crdt.smallestScore = copy
		} else {
			effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
		}*/
		*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
		//new top, may update min
		return effect
	}

	//Top is full and the element isn't there
	entry, has = crdt.notInTop[op.Id]
	nonPropEntry, nonPropHas := crdt.notPropagated[op.Id]
	negScore := TopKScore{Id: op.Id, Score: -op.Score, Data: op.Data}
	if has {
		//No need to propagate, storing the decrement is enough. It might be relevant if the element ever gets to the top.
		//Case 4: element wasn't on top, so it won't be now for sure
		oldEntry := *entry
		entry.Score -= op.Score
		crdt.highestNotTop.updateLower(*entry, oldEntry, crdt)
		effect = TopSumAddNotTopEffect{newScore: *entry, oldScore: oldEntry.Score}
		if nonPropHas {
			nonPropEntry.Score -= op.Score
		} else {
			crdt.notPropagated[op.Id] = &negScore
		}
		if !crdt.highestNotTop.hasMax() {
			crdt.findAndUpdateNotTopMax()
		}
		/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
		if !ok {
			panic(fmt.Sprintf("Old score: %v. New score: %v. Error: %s.\n", oldEntry, *entry, err))
		}*/
		//Decrement to non-top
		return effect
	}

	min := crdt.smallestScores.getMin()
	//Case 5: element doesn't exist. It may need to go for top
	if negScore.isHigher(&min) {
		//top. Must propagate.
		crdt.elems[op.Id] = &negScore
		delete(crdt.elems, min.Id)
		//TopSumAddReplace because this element is being added to top;
		effect = TopSumAddReplaceEffect{newElem: negScore, newElemOldScore: math.MinInt32, oldElem: min}
		crdt.notInTop[min.Id] = &min
		crdt.highestNotTop.addMax(min)
		crdt.smallestScores.removeMin()
		crdt.smallestScores.addIfInBetween(negScore, len(crdt.elems))
		if !crdt.smallestScores.hasMin() {
			crdt.findAndUpdateMin()
		}
		/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
		if !ok {
			panic(fmt.Sprintf("New score: %v. Old min: %v. New min: %v. Error: %s.\n", negEntry, min, crdt.smallestScores.getMin(), err))
		}*/
		//crdt.findAndUpdateMin()
		*op.replicate, crdt.nUpds, crdt.sortedElems = true, crdt.nUpds+1, nil
		return effect
	}

	//not top. Don't propagate.
	crdt.notInTop[op.Id] = &negScore
	crdt.notPropagated[op.Id] = negScore.copy()
	crdt.highestNotTop.addIfInBetween(negScore, len(crdt.notInTop))
	/*err, ok := sanityCheckBuffers(crdt.smallestScores, crdt.highestNotTop)
	if !ok {
		panic(fmt.Sprintf("New score: %v. Error: %s.\n", negScore, err))
	}*/
	//New element to non-top
	effect = TopSumAddNotTopEffect{newScore: negScore, oldScore: math.MinInt32}
	return effect
}

func (crdt *TopSumCrdt) applyTopSSubAllDownstreamArgs(op DownstreamTopSSubAll) (effect Effect) {
	scores := *op.Scores
	if len(scores) == 0 {
		return NoEffect{}
	}
	downScores, effects := make([]TopKScore, len(scores)), make(TopSumMultiEffect, len(scores))
	downI := 0
	var currScore TopKScore
	i := 0
	changedTop := false

	crdt.resizeTopSumIfBigUpdate(len(*op.Scores))
	//Case 1: no elements yet
	if len(crdt.elems) == 0 {
		currScore = scores[0]
		negScore := TopKScore{Id: currScore.Id, Score: -currScore.Score, Data: currScore.Data}
		i++
		crdt.smallestScores.add(negScore)
		crdt.elems[currScore.Id] = &negScore
		downScores[downI], downI, changedTop = currScore, downI+1, true
		effects[i] = TopSumAddEffect{newScore: negScore, oldScore: math.MinInt32}
	}
	var entry, nonPropEntry *TopKScore
	var has, nonPropHas bool
	var min TopKScore
	for ; i < len(scores); i++ {
		currScore, min = scores[i], crdt.smallestScores.getMin()
		entry, has = crdt.elems[currScore.Id]
		if has {
			oldEntry := *entry
			//Case 2: decrease to element in top, thus it must be propagated
			entry.Score -= currScore.Score
			if min.isHigher(entry) {
				if len(crdt.notInTop) == 0 || crdt.highestNotTop.getMax().isLowerScore(*entry) {
					effects[i] = TopSumAddEffect{newScore: *entry, oldScore: oldEntry.Score}
					//Special case, no move but it becomes the smallest score
					crdt.smallestScores.updateLower(*entry, oldEntry)
				} else {
					//Move to notTop, find new min, add that min to top
					newMin := crdt.highestNotTop.getMax()
					crdt.highestNotTop.removeMax()
					crdt.highestNotTop.addIfInBetween(*entry, len(crdt.notInTop))
					oldScore := TopKScore{Id: entry.Id, Score: entry.Score + currScore.Score}
					crdt.smallestScores.remove(oldScore)
					crdt.smallestScores.addMin(newMin)
					//Searching for new min in nonTop; this min will be added to elems
					/*newMin := &TopKScore{Id: math.MinInt32, Score: math.MinInt32}
					for _, elem := range crdt.notInTop {
						if elem.isHigher(newMin) {
							newMin = elem
						}
					}*/
					delete(crdt.elems, entry.Id)
					delete(crdt.notInTop, newMin.Id)
					crdt.notInTop[entry.Id] = entry
					crdt.elems[newMin.Id] = &newMin
					//Must be after we update crdt.notInTop, to prevent re-adding an element we just removed.
					if !crdt.highestNotTop.hasMax() {
						crdt.findAndUpdateNotTopMax()
					}
					//crdt.smallestScore = newMin
					effects[i] = TopSumSubReplaceEffect{newElem: *entry, oldElem: newMin, newElemOldScore: oldScore.Score}
				}
			} else { //Stays on top.
				effects[i] = TopSumAddEffect{newScore: *entry, oldScore: oldEntry.Score}
				crdt.smallestScores.updateLower(*entry, oldEntry) //Now it may belong to the min, or just need its position updated.
			}
			downScores[downI], downI, changedTop = currScore, downI+1, true
		} else if len(crdt.elems) < crdt.maxElems {
			//Case 3: has space on top, and element doesn't yet exist
			newScore := TopKScore{Id: currScore.Id, Score: -currScore.Score, Data: currScore.Data}
			crdt.elems[currScore.Id] = &newScore
			crdt.smallestScores.addIfInBetween(newScore, len(crdt.elems))
			/*if crdt.smallestScore.isHigher(newScore) {
				crdt.smallestScores.updateMin(newScore)
			}*/
			effects[i] = TopSumAddEffect{newScore: newScore, oldScore: math.MinInt32}
			downScores[downI], downI, changedTop = currScore, downI+1, true
		} else {
			//Top is full and element isn't there (thus, entry is nil)
			entry, has = crdt.notInTop[currScore.Id]
			nonPropEntry, nonPropHas = crdt.notPropagated[currScore.Id]
			negScore := TopKScore{Id: currScore.Id, Score: -currScore.Score, Data: currScore.Data}
			if has {
				oldEntry := *entry
				//No need to propagate, storing the decrement is enough. It might be relevant if the element ever gets to the top.
				//Case 4: element wasn't on top, so it won't be now for sure
				entry.Score -= currScore.Score
				effects[i] = TopSumAddNotTopEffect{newScore: *entry, oldScore: oldEntry.Score}
				if nonPropHas {
					nonPropEntry.Score -= currScore.Score
				} else {
					crdt.notPropagated[currScore.Id] = &negScore
				}
				crdt.highestNotTop.updateLower(*entry, oldEntry, crdt)
				if !crdt.highestNotTop.hasMax() {
					crdt.findAndUpdateNotTopMax()
				}
			} else if negScore.isHigher(&min) { //Can't use entry, as !has, so entry is nil. Intended is negScore.
				//else if entry.isHigher(crdt.smallestScore) {
				//Case 5: element doesn't exist, but needs to go to top
				crdt.elems[currScore.Id] = &negScore
				delete(crdt.elems, min.Id)
				//TopSumAddReplace because this element is being added to top;
				effects[i] = TopSumAddReplaceEffect{newElem: negScore, newElemOldScore: math.MinInt32, oldElem: min}
				crdt.notInTop[min.Id] = min.copy()
				crdt.highestNotTop.addMax(min)
				crdt.smallestScores.removeMin()
				crdt.smallestScores.addIfInBetween(negScore, len(crdt.elems))
				if !crdt.smallestScores.hasMin() {
					crdt.findAndUpdateMin()
				}
				//crdt.findAndUpdateMin()
				downScores[downI], downI, changedTop = currScore, downI+1, true
			} else {
				//Not top. Also don't propagate.
				crdt.notInTop[currScore.Id] = &negScore
				crdt.notPropagated[currScore.Id] = negScore.copy()
				crdt.highestNotTop.addIfInBetween(negScore, len(crdt.notInTop))
				effects[i] = TopSumAddNotTopEffect{newScore: negScore, oldScore: math.MinInt32}
			}
		}
	}
	if downI > 0 {
		downScores = downScores[:downI]
	}
	if changedTop {
		crdt.nUpds++
		crdt.sortedElems = nil
	}
	*op.Scores = downScores
	//fmt.Printf("[TopSum][Dec]Top, notTop sizes: %d, %d. nMin, notTopMax calcs: %d, %d. nMin, notTopMax buffer sizes: %d, %d.\n",
	//	len(crdt.elems), len(crdt.notInTop), crdt.nMin, crdt.nMaxNotTop, crdt.smallestScores.Cap(), crdt.highestNotTop.Cap())
	return effects
}

/*func (crdt *TopSumCrdt) shouldReplicate(existing int32, notReplicated int32) bool {
	//If every replica added as much as us, they it would be added to the top
	return notReplicated*NReplicas > crdt.smallestScore.Score-existing
}*/

func (crdt *TopSumCrdt) shouldReplicate(existing int32, notReplicated int32, minScore int32) bool {
	//If every replica added as much as us, they it would be added to the top
	return notReplicated*NReplicas > minScore-existing
}

/*func (crdt *TopSumCrdt) findAndUpdateMin() {
	minSoFar := TopKScore{Id: math.MaxInt32, Score: math.MaxInt32}
	for _, elem := range crdt.elems {
		if minSoFar.isHigher(elem) {
			minSoFar = *elem
		}
	}
	crdt.smallestScore = &minSoFar
}*/

func (crdt *TopSumCrdt) findAndUpdateMin() {
	if len(crdt.elems) <= 1500 { //Cheaper to copy to a slice and then sort.
		elemsSlice := make([]TopKScore, len(crdt.elems))
		i := 0
		for _, elem := range crdt.elems {
			elemsSlice[i] = *elem
			i++
		}
		slices.SortFunc(elemsSlice, func(a, b TopKScore) int {
			if a.Score != b.Score {
				return int(a.Score - b.Score)
			}
			return int(a.Id - b.Id)
		})
		crdt.smallestScores.copyFrom(elemsSlice)
	} else { //Need a heap.
		bufSize := crdt.smallestScores.Cap()
		h := newMaxHeapTopKScore(bufSize)
		heap.Init(h)
		for _, elem := range crdt.elems {
			if h.Len() < bufSize {
				heap.Push(h, *elem)
			} else if elem.isSmaller(&(*h)[0]) {
				heap.Pop(h)
				heap.Push(h, *elem)
			}
		}
		minSlice := crdt.smallestScores.mins[:bufSize]
		for i := len(minSlice) - 1; i >= 0; i-- {
			minSlice[i] = heap.Pop(h).(TopKScore)
		}
		crdt.smallestScores.size = bufSize
	}
	crdt.nMin++
}

func (crdt *TopSumCrdt) findAndUpdateNotTopMax() {
	crdt.resizeNotTopMaxIfNeeded()
	if len(crdt.notInTop) <= 1500 {
		elemsSlice := make([]TopKScore, len(crdt.notInTop))
		i := 0
		for _, entry := range crdt.notInTop {
			elemsSlice[i] = *entry
			i++
		}
		elemsSlice = elemsSlice[:i]
		slices.SortFunc(elemsSlice, func(a, b TopKScore) int {
			if a.Score != b.Score {
				return int(b.Score - a.Score)
			}
			return int(b.Id - a.Id)
		})
		crdt.highestNotTop.copyFrom(elemsSlice)
	} else { //Need a heap.
		bufSize := crdt.highestNotTop.Cap()
		h := newMinHeapTopKScore(bufSize)
		heap.Init(h)
		for _, entry := range crdt.notInTop {
			if h.Len() < bufSize {
				heap.Push(h, *entry)
			} else if entry.isHigher(&(*h)[0]) {
				heap.Pop(h)
				heap.Push(h, *entry)
			}
		}
		maxSlice := crdt.highestNotTop.maxs[:bufSize]
		for i := len(maxSlice) - 1; i >= 0; i-- {
			maxSlice[i] = heap.Pop(h).(TopKScore)
		}
		crdt.highestNotTop.size = bufSize
	}
	crdt.nMaxNotTop++
	for id, entry := range crdt.elems {
		if id != entry.Id {
			panic(fmt.Sprintf("[TOPSUM][findAndUpdateNotTopMax]Found inconsistency in crdt.elems: id (key) doesn't match entry.id. len elems: %d. id (key): %d. entry: %v.\n", len(crdt.elems), id, entry))
		}
	}
	for id, entry := range crdt.notInTop {
		if id != entry.Id {
			panic(fmt.Sprintf("[TOPSUM][findAndUpdateNotTopMax]Found inconsistency in crdt.notInTop: id (key) doesn't match entry.id. len notInTop: %d. id (key): %d. entry: %v.\n", len(crdt.notInTop), id, entry))
		}
	}
}

func (crdt *TopSumCrdt) resizeNotTopMaxIfNeeded() (resized bool) {
	//Since we don't have a limit to notInTop's size, we will grow the buffer's size accordingly.
	//If the notInTop size shrinks too much, we will also decrease it.
	notTopSize := float64(len(crdt.notInTop))
	recSize := tools.Min(tools.Max(minBufferSize, int(notTopSize*MAX_NOT_TOP_BUF_FACTOR)), 200) //TODO: We may have to take away this 200 cap, or consider increasing it (slowly) when we detect a very, very large notInTop.
	if recSize > int(float64(crdt.highestNotTop.Cap())*1.1) {                                   //We'll resize it as notInTop grows, but if it changes lightly we keep the same size.
		crdt.highestNotTop, resized = newMaxBuffer(recSize, MIN_SCORE), true
	} else if recSize < crdt.highestNotTop.Cap()/2 { //NotInTop shrank considerably (or few compared to elems in top), shrink this too.
		crdt.highestNotTop, resized = newMaxBuffer(recSize, MIN_SCORE), true
	}
	return resized
}

// This function is a helper for preparing the scores for a TopSAddAndSubAll. It puts all negative scores at the end of the slice (and converts them to positive), and returns how many positive scores there are.
func sortTopScoreEntriesPosNeg(entries []TopKScore) (nPositive int) {
	negStart, currSearchPos := len(entries), 0
	for i, score := range entries {
		if score.Score < 0 { //Trade with last non-negative score. If that one is also negative, we keep iterating until we find a positive or we reach i.
			for currSearchPos = negStart - 1; currSearchPos > i && entries[currSearchPos].Score < 0; currSearchPos-- { //Iterate until we find a non-negative
			}
			if currSearchPos >= i { //We found a negative.
				negStart = currSearchPos
			}
			//Swap the negative score with the last non-negative score
			if i >= currSearchPos { //Rearranging complete: all scores after i are negative. i can be > currSearchPos in a specific case: currSearchPos = i + 1, we swap both, next iteration i will be equal to negStart and, thus 1 above currSearchPos.
				break
			}
			entries[i], entries[currSearchPos] = entries[currSearchPos], entries[i]
		}
	}
	//Iterate all negative entries and swap them to positive.
	//We can't change negatives in the cycle above as that would lead to some elements being wrongly recognized as positive (and some wouldn't be converted too)
	for i := negStart; i < len(entries); i++ {
		entries[i].Score = -entries[i].Score
	}
	return negStart //negStart is our number of positive elements (i.e., positive are in the range of [0:negStart], and negatives start at [negStart:])
}

func (crdt *TopSumCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

//METHODS FOR INVERSIBLE CRDT

func (crdt *TopSumCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := TopSumCrdt{
		CRDTVM:         crdt.CRDTVM.copy(),
		maxElems:       crdt.maxElems,
		smallestScores: crdt.smallestScores.copy(),
		elems:          tools.MapCopy(crdt.elems),
		notInTop:       tools.MapCopy(crdt.notInTop),
		notPropagated:  tools.MapCopy(crdt.notPropagated),
		highestNotTop:  crdt.highestNotTop.copy(),
		nReads:         crdt.nReads,
		nUpds:          crdt.nUpds,
	}

	return &newCrdt
}

func (crdt *TopSumCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *TopSumCrdt) reapplyOp(updArgs DownstreamArguments) (effect Effect) {
	effect, _ = crdt.applyDownstream(updArgs)
	return
}

func (crdt *TopSumCrdt) undoEffect(effect Effect) {
	switch typedEffect := (effect).(type) {
	case TopSumAddEffect:
		crdt.undoAddEffect(&typedEffect)
	case TopSumAddNotTopEffect:
		crdt.undoAddNotTopEffect(&typedEffect)
	case TopSumAddReplaceEffect:
		crdt.undoAddReplaceEffect(&typedEffect)
	case TopSumSubReplaceEffect:
		crdt.undoSubReplaceEffect(&typedEffect)
	case TopSumMultiEffect:
		for _, singleEffect := range typedEffect {
			crdt.undoEffect(&singleEffect)
		}
	}
}

/*func (crdt *TopSumCrdt) undoAddEffect(effect *TopSumAddEffect) {
	if effect.oldScore == math.MinInt32 {
		//Element didn't exist before
		delete(crdt.elems, effect.newScore.Id)
	} else {
		crdt.elems[effect.newScore.Id].Score = effect.oldScore
	}
	if (effect.oldMin != TopKScore{}) {
		crdt.smallestScore = &effect.oldMin
	}
}

func (crdt *TopSumCrdt) undoAddReplaceEffect(effect *TopSumAddReplaceEffect) {
	//Common to all cases
	delete(crdt.notInTop, effect.oldElem.Id)
	crdt.elems[effect.oldElem.Id] = &effect.oldElem
	crdt.smallestScore = &effect.oldElem
	if effect.newElemOldScore == math.MinInt32 {
		//Element didn't exist before. Only thing left is to delete the element from top
		delete(crdt.elems, effect.newElem.Id)
	} else {
		//Need to move element to notTop, with its old value
		newNotTop := crdt.elems[effect.newElem.Id]
		newNotTop.Score = effect.newElemOldScore
		crdt.notInTop[effect.newElem.Id] = newNotTop
		delete(crdt.elems, effect.newElem.Id)
	}
}

func (crdt *TopSumCrdt) undoAddNotTopEffect(effect *TopSumAddNotTopEffect) {
	if effect.oldScore == math.MinInt32 {
		//Element didn't exist before
		delete(crdt.elems, effect.newScore.Id)
	} else {
		crdt.notInTop[effect.newScore.Id].Score = effect.oldScore
	}
}

func (crdt *TopSumCrdt) undoSubReplaceEffect(effect *TopSumSubReplaceEffect) {
	//Only one case (top was full; element on top went to notInTop from a decrease)
	newTop := effect.newElem
	newTop.Score = effect.newElemOldScore
	//newTop was the one that got moved from top to notInTop before
	crdt.elems[newTop.Id] = &newTop
	newNotTop := effect.oldElem
	crdt.notInTop[newNotTop.Id] = &newNotTop
	newMin := effect.oldMin
	crdt.smallestScore = &newMin
}
*/

func (crdt *TopSumCrdt) undoAddEffect(effect *TopSumAddEffect) {
	if effect.oldScore == math.MinInt32 { //New element.
		delete(crdt.elems, effect.newScore.Id)
		crdt.smallestScores.remove(effect.newScore)
	} else { //Increment (or decrement) to existing element.
		crdt.elems[effect.newScore.Id].Score = effect.oldScore
		copyScore := effect.newScore
		copyScore.Score = effect.oldScore
		crdt.smallestScores.update(copyScore, effect.newScore) //We call update as we don't know if it was an increment or decrement.
	}
}

func (crdt *TopSumCrdt) undoAddNotTopEffect(effect *TopSumAddNotTopEffect) {
	//Cases: 1) increment to existing notTop; 2) new element to notTop
	copy := effect.newScore //This copy will be needed to update highestNotTop.
	copy.Score = effect.oldScore
	if effect.oldScore == math.MinInt32 { //Case 2: new element to notTop
		delete(crdt.notInTop, effect.newScore.Id)
		crdt.highestNotTop.remove(copy)
	} else { //Case 1: increment to existing notTop
		crdt.notInTop[effect.newScore.Id].Score = effect.oldScore
		crdt.highestNotTop.updateLower(copy, effect.newScore, crdt)
	}
}

func (crdt *TopSumCrdt) undoAddReplaceEffect(effect *TopSumAddReplaceEffect) {
	//Two cases: 1) move a notTop element to top by increment; 2) new element that goes directly to the top (despite top being full)
	//Common to both cases
	delete(crdt.elems, effect.newElem.Id)
	oldElem := effect.oldElem //Ensuring we copy, to avoid issues with pointers later.
	crdt.elems[effect.oldElem.Id] = &oldElem
	crdt.smallestScores.remove(effect.newElem)
	crdt.smallestScores.addMin(effect.oldElem)
	crdt.highestNotTop.removeMax()

	if effect.newElemOldScore != math.MinInt32 { //1) - move a nonTop element to top by increment (kicking another element down)
		copyNew := effect.newElem
		copyNew.Score = effect.newElemOldScore
		crdt.notInTop[effect.newElem.Id] = &copyNew
		crdt.highestNotTop.addIfInBetween(copyNew, len(crdt.notInTop)) //Will never kick out anything, as we just did removeMax() before.
	} //else: 2) - new elem that goes directly to top, kicking another element down. Nothing extra to do in this case.
}

func (crdt *TopSumCrdt) undoSubReplaceEffect(effect *TopSumSubReplaceEffect) {
	//Only one case: top was full, element on top went to notInTop due to a decrease.
	//The other case (new elem goes directly to top despite it being full) is handled by TopSumAddReplaceEffect.
	crdt.elems[effect.newElem.Id].Score = effect.newElemOldScore
	oldElem := effect.oldElem //Ensuring we copy, to avoid issues with pointers later.
	crdt.elems[effect.oldElem.Id] = &oldElem
	crdt.highestNotTop.remove(effect.newElem) //addIfInBetween(entry) -> remove(newElem)
	crdt.highestNotTop.addMax(effect.oldElem) //removeMax() (newMin) -> addMax(oldElem)
	crdt.smallestScores.removeMin()           //addMin(newMin) -> removeMin()
	copyWithOldScore := effect.newElem
	copyWithOldScore.Score = effect.newElemOldScore
	crdt.smallestScores.addIfInBetween(copyWithOldScore, len(crdt.elems)) //remove(oldScore) -> addIfInBetween(copyWithOldScore)
}

func (crdt *TopSumCrdt) notifyRebuiltComplete(currTs clocksi.Timestamp) {}

// Protobuf function
func (crdtOp TopSAdd) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	/*add := protobuf.GetTopkrmvop().GetAdds()[0]
	crdtOp.TopKScore = TopKScore{Id: add.GetPlayerId(), Score: add.GetScore(), Data: emptyData}
	if add.Data != nil {
		crdtOp.Data = tools.NewByteSlicePtr(add.Data)
	}*/
	add := protobuf.GetTopkrmvop().GetAdds()
	crdtOp.TopKScore = TopKScore{Id: add.PlayerIds[0], Score: add.Scores[0], Data: emptyData}
	if len(add.Data) > 0 {
		crdtOp.TopKScore.Data = tools.NewByteSlicePtr(add.Data[0])
	}
	return crdtOp
}

func (crdtOp TopSAdd) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	/*add := proto.ApbIntPair{PlayerId: pb.Int32(crdtOp.Id), Score: pb.Int32(crdtOp.Score)}
	if crdtOp.Data != nil && len(*crdtOp.Data) > 0 {
		add.Data = *crdtOp.Data
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: []*proto.ApbIntPair{&add}, PositiveLen: POINTER_ONE_UINT32}}}*/
	add := proto.ApbTopKRmvAdd{PlayerIds: []int32{crdtOp.Id}, Scores: []int32{crdtOp.Score}}
	if crdtOp.Data != nil && len(*crdtOp.Data) > 0 {
		add.Data = [][]byte{*crdtOp.Data}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: &add, PositiveLen: POINTER_ONE_UINT32}}}
}

func (crdtOp TopSSub) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	/*sub := protobuf.GetTopkrmvop().GetAdds()[0]
	crdtOp.TopKScore = TopKScore{Id: sub.GetPlayerId(), Score: -sub.GetScore(), Data: emptyData}
	if sub.Data != nil {
		crdtOp.Data = tools.NewByteSlicePtr(sub.Data)
	}*/
	sub := protobuf.GetTopkrmvop().GetAdds()
	crdtOp.TopKScore = TopKScore{Id: sub.PlayerIds[0], Score: -sub.Scores[0], Data: emptyData}
	if len(sub.Data) > 0 {
		crdtOp.TopKScore.Data = tools.NewByteSlicePtr(sub.Data[0])
	}
	return crdtOp
}

func (crdtOp TopSSub) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	/*sub := proto.ApbIntPair{PlayerId: pb.Int32(crdtOp.Id), Score: pb.Int32(-crdtOp.Score)} //We write negative into the protobuf, so that it can be recognized as a sub.
	if crdtOp.Data != nil && len(*crdtOp.Data) > 0 {
		sub.Data = *crdtOp.Data
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: []*proto.ApbIntPair{&sub}, PositiveLen: POINTER_ZERO_UINT32}}}*/
	sub := proto.ApbTopKRmvAdd{PlayerIds: []int32{crdtOp.Id}, Scores: []int32{-crdtOp.Score}} //We write negative into the protobuf, so that it can be recognized as a sub.
	if crdtOp.Data != nil && len(*crdtOp.Data) > 0 {
		sub.Data = [][]byte{*crdtOp.Data}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: &sub, PositiveLen: POINTER_ZERO_UINT32}}}
}

func (crdtOp TopSAddAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	adds := protobuf.GetTopkrmvop().GetAdds()
	ids, scores, data := adds.GetPlayerIds(), adds.GetScores(), adds.GetData()
	crdtOp.Scores = make([]TopKScore, len(ids))
	if len(data) == 0 {
		for i, id := range ids {
			crdtOp.Scores[i] = TopKScore{Id: id, Score: scores[i], Data: emptyData}
		}
	} else {
		for i, id := range ids {
			crdtOp.Scores[i] = TopKScore{Id: id, Score: scores[i], Data: tools.NewByteSlicePtr(data[i])}
		}
	}
	/*var currScore TopKScore
	crdtOp.Scores = make([]TopKScore, len(adds))
	for i, add := range adds {
		currScore = TopKScore{Id: add.GetPlayerId(), Score: add.GetScore(), Data: emptyData}
		if add.Data != nil {
			currScore.Data = tools.NewByteSlicePtr(add.Data)
		}
		crdtOp.Scores[i] = currScore
	}*/
	return crdtOp

}

func (crdtOp TopSAddAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	/*protoAdds := make([]*proto.ApbIntPair, len(crdtOp.Scores))
	for i, score := range crdtOp.Scores {
		add := proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(score.Score)}
		if score.Data != nil && len(*score.Data) > 0 {
			add.Data = *score.Data
		}
		protoAdds[i] = &add
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: protoAdds, PositiveLen: pb.Uint32(uint32(len(crdtOp.Scores)))}}}*/
	ids, scores := make([]int32, len(crdtOp.Scores)), make([]int32, len(crdtOp.Scores))
	var data [][]byte //We'll only allocate this if at least one entry has data set.
	for i, score := range crdtOp.Scores {
		ids[i], scores[i] = score.Id, score.Score
		if score.Data != nil && len(*score.Data) > 0 {
			if data == nil {
				data = make([][]byte, len(crdtOp.Scores))
			}
			data[i] = *score.Data
		}
	}
	protoAdds := &proto.ApbTopKRmvAdd{PlayerIds: ids, Scores: scores, Data: data}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: protoAdds, PositiveLen: pb.Uint32(uint32(len(crdtOp.Scores)))}}}
}

func (crdtOp TopSSubAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	subs := protobuf.GetTopkrmvop().GetAdds()
	ids, scores, data := subs.GetPlayerIds(), subs.GetScores(), subs.GetData()
	crdtOp.Scores = make([]TopKScore, len(ids))
	if len(data) == 0 {
		for i, id := range ids {
			crdtOp.Scores[i] = TopKScore{Id: id, Score: -scores[i], Data: emptyData}
		}
	} else {
		for i, id := range ids {
			crdtOp.Scores[i] = TopKScore{Id: id, Score: -scores[i], Data: tools.NewByteSlicePtr(data[i])}
		}
	}
	/*crdtOp.Scores = make([]TopKScore, len(subs))
	var currScore TopKScore
	for i, sub := range subs {
		currScore = TopKScore{Id: sub.GetPlayerId(), Score: -sub.GetScore(), Data: emptyData}
		if sub.Data != nil {
			currScore.Data = tools.NewByteSlicePtr(sub.Data)
		}
		crdtOp.Scores[i] = currScore
	}*/
	return crdtOp
}

func (crdtOp TopSSubAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	/*protoSubs := make([]*proto.ApbIntPair, len(crdtOp.Scores))
	for i, score := range crdtOp.Scores {
		sub := proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(-score.Score)}
		if score.Data != nil && len(*score.Data) > 0 {
			sub.Data = *score.Data
		}
		protoSubs[i] = &sub
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: protoSubs, PositiveLen: POINTER_ZERO_UINT32}}}
	*/
	ids, scores := make([]int32, len(crdtOp.Scores)), make([]int32, len(crdtOp.Scores))
	var data [][]byte //We'll only allocate this if at least one entry has data set.
	for i, score := range crdtOp.Scores {
		ids[i], scores[i] = score.Id, -score.Score
		if score.Data != nil && len(*score.Data) > 0 {
			if data == nil {
				data = make([][]byte, len(crdtOp.Scores))
			}
			data[i] = *score.Data
		}
	}
	protoSubs := &proto.ApbTopKRmvAdd{PlayerIds: ids, Scores: scores, Data: data}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: protoSubs, PositiveLen: POINTER_ZERO_UINT32}}}
}

func (crdtOp TopSAddAndSubAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	topProto := protobuf.GetTopkrmvop()
	protoElems, nPositive := topProto.GetAdds(), int(topProto.GetPositiveLen())
	ids, scores, data := protoElems.GetPlayerIds(), protoElems.GetScores(), protoElems.GetData()
	crdtOp.AddScores, crdtOp.SubScores = make([]TopKScore, nPositive), make([]TopKScore, len(ids)-nPositive)
	if len(data) == 0 {
		for i := 0; i < nPositive; i++ {
			crdtOp.AddScores[i] = TopKScore{Id: ids[i], Score: scores[i], Data: emptyData}
		}
		for i := nPositive; i < len(ids); i++ {
			crdtOp.SubScores[i-nPositive] = TopKScore{Id: ids[i], Score: -scores[i], Data: emptyData}
		}
	} else {
		for i := 0; i < nPositive; i++ {
			crdtOp.AddScores[i] = TopKScore{Id: ids[i], Score: scores[i], Data: tools.NewByteSlicePtr(data[i])}
		}
		for i := nPositive; i < len(ids); i++ {
			crdtOp.SubScores[i-nPositive] = TopKScore{Id: ids[i], Score: -scores[i], Data: tools.NewByteSlicePtr(data[i])}
		}
	}
	/*
		var currElem *proto.ApbIntPair
		for i := 0; i < nPositive; i++ {
			currElem = protoElems[i]
			currScore = TopKScore{Id: currElem.GetPlayerId(), Score: currElem.GetScore(), Data: emptyData}
			if currElem.Data != nil {
				currScore.Data = tools.NewByteSlicePtr(currElem.Data)
			}
			crdtOp.AddScores[i] = currScore
			//if currScore.Score < 0 {
			//	fmt.Printf("[TopSAddAndSubAll][FromUpdateObject]Warning: element %+v, index %d, of adds is negative! NElems (add/sub/total): %d/%d/%d.\n",
			//		currScore, i, nPositive, len(protoElems)-nPositive, len(protoElems))
			//}
		}
		for i := nPositive; i < len(protoElems); i++ {
			currElem = protoElems[i]
			currScore = TopKScore{Id: currElem.GetPlayerId(), Score: -currElem.GetScore(), Data: emptyData}
			if currElem.Data != nil {
				currScore.Data = tools.NewByteSlicePtr(currElem.Data)
			}
			crdtOp.SubScores[i-nPositive] = currScore
			//if currScore.Score < 0 {
			//	fmt.Printf("[TopSAddAndSubAll][FromUpdateObject]Warning: element %+v, index %d, of subs is negative! NElems (add/sub/total): %d/%d/%d.\n",
			//		currScore, i, nPositive, len(protoElems)-nPositive, len(protoElems))
			//}
		}*/
	return crdtOp
}

func (crdtOp TopSAddAndSubAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	totalLen := len(crdtOp.AddScores) + len(crdtOp.SubScores)
	ids, scores := make([]int32, totalLen), make([]int32, totalLen)
	var data [][]byte //We'll only allocate this if at least one entry has data set.
	for i, score := range crdtOp.AddScores {
		ids[i], scores[i] = score.Id, score.Score
		if score.Data != nil && len(*score.Data) > 0 {
			if data == nil {
				data = make([][]byte, totalLen)
			}
			data[i] = *score.Data
		}
	}
	offset := len(crdtOp.AddScores)
	for i, score := range crdtOp.SubScores {
		ids[offset+i], scores[offset+i] = score.Id, -score.Score
		if score.Data != nil && len(*score.Data) > 0 {
			if data == nil {
				data = make([][]byte, totalLen)
			}
			data[offset+i] = *score.Data
		}
	}
	protoElems := &proto.ApbTopKRmvAdd{PlayerIds: ids, Scores: scores, Data: data}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: protoElems, PositiveLen: pb.Uint32(uint32(offset))}}}

	/*protoElems := make([]*proto.ApbIntPair, len(crdtOp.AddScores)+len(crdtOp.SubScores))
	for i, score := range crdtOp.AddScores {
		currElem := proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(score.Score)}
		if score.Data != nil && len(*score.Data) > 0 {
			currElem.Data = *score.Data
		}
		protoElems[i] = &currElem
		//if score.Score < 0 {
		//	fmt.Printf("[TopSAddAndSubAll][ToUpdateObject]Warning: element %+v, index %d, of adds is negative! NElems (add/sub/total): %d/%d/%d.\n",
		//		score, i, len(crdtOp.AddScores), len(crdtOp.SubScores), len(protoElems))
		//}
	}
	offset := len(crdtOp.AddScores)
	for i, score := range crdtOp.SubScores {
		currElem := proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(-score.Score)}
		if score.Data != nil && len(*score.Data) > 0 {
			currElem.Data = *score.Data
		}
		protoElems[offset+i] = &currElem
		//if score.Score < 0 {
		//	fmt.Printf("[TopSAddAndSubAll][ToUpdateObject]Warning: element %+v, index %d, of subs is negative! NElems (add/sub/total): %d/%d/%d.\n",
		//		score, i, len(crdtOp.AddScores), len(crdtOp.SubScores), len(protoElems))
		//}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkrmvop{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: protoElems, PositiveLen: pb.Uint32(uint32(offset))}}}*/
}

func (crdtOp TopSInit) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return TopSInit(protobuf.GetTopkinitop().GetTopSize())
}

func (crdtOp TopSInit) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkinitop{Topkinitop: &proto.ApbTopKInit{TopSize: pb.Uint32(uint32(crdtOp)), TopType: proto.CRDTType_TOPSUM.Enum()}}}
}

// Same as for TopK
func (crdtState TopSValueState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	protoScores := protobuf.GetTopk()
	if len(protoScores.PlayerIds) == 0 {
		//Partial read
		protoScores = protobuf.GetPartread().GetTopk().GetPairs()
	}
	ids, scores, data := protoScores.GetPlayerIds(), protoScores.GetScores(), protoScores.GetData()
	crdtState.Scores = make([]TopKScore, len(ids))
	if len(data) == 0 {
		for i, id := range ids {
			crdtState.Scores[i] = TopKScore{Id: id, Score: scores[i]}
		}
	} else {
		for i, id := range ids {
			crdtState.Scores[i] = TopKScore{Id: id, Score: scores[i], Data: &data[i]}
		}
	}
	return crdtState

	/*protoScores := protobuf.GetTopk().GetValues()
	if protoScores == nil {
		//Partial read
		protoScores = protobuf.GetPartread().GetTopk().GetPairs().GetValues()
	}
	crdtState.Scores = make([]TopKScore, len(protoScores))
	for i, pair := range protoScores {
		data := pair.GetData()
		crdtState.Scores[i] = TopKScore{Id: pair.GetPlayerId(), Score: pair.GetScore(), Data: &data}
	}
	return crdtState*/
}

// Same as for TopK
func (crdtState TopSValueState) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	var ids, scores []int32
	topKBuf := TopStateBufs{}
	if len(crdtState.Scores) >= shared.MIN_SLICE_POOL_SIZE {
		ids, scores = int32SlicePool.Get(len(crdtState.Scores)), int32SlicePool.Get(len(crdtState.Scores))
		topKBuf.Ids, topKBuf.Scores = ids, scores
	} else {
		ids, scores = make([]int32, len(crdtState.Scores)), make([]int32, len(crdtState.Scores))
	}
	var data [][]byte //We'll only allocate this if needed.
	for i, score := range crdtState.Scores {
		ids[i], scores[i] = score.Id, score.Score
		if score.Data != nil && len(*score.Data) > 0 {
			if data == nil {
				if len(crdtState.Scores) >= shared.MIN_SLICE_POOL_SIZE {
					data = bytesSlicePool.Get(len(crdtState.Scores))
					topKBuf.DataBuf = data
				} else {
					data = make([][]byte, len(crdtState.Scores))
				}
			}
			data[i] = *score.Data
		}
	}

	if len(ids) >= shared.MIN_SLICE_POOL_SIZE {
		buf.AddBufToReturn(crdtState.GetCRDTType(), topKBuf)
	}
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Topk{Topk: &proto.ApbGetTopkResp{PlayerIds: ids, Scores: scores, Data: data}}}
	/*protos := make([]*proto.ApbIntPair, len(crdtState.Scores))
	for i, score := range crdtState.Scores {
		protos[i] = &proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(score.Score), Data: *score.Data}
	}
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Topk{Topk: &proto.ApbGetTopkResp{Values: protos}}}*/
}

func (downOp DownstreamTopSAdd) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	elem := protobuf.GetTopsumOp().GetElems()[0]
	downOp.Id, downOp.Score, downOp.Data, downOp.replicate = elem.GetId(), elem.GetScore(), emptyData, new(bool)
	if elem.Data != nil {
		downOp.Data = tools.NewByteSlicePtr(elem.Data)
	}
	return downOp
}

func (downOp DownstreamTopSAdd) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	elem := &proto.ProtoTopSumElement{Id: pb.Int32(downOp.Id), Score: pb.Int32(downOp.Score)}
	if downOp.Data != nil && len(*downOp.Data) > 0 {
		elem.Data = *downOp.Data
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopsumOp{TopsumOp: &proto.ProtoTopSumDownstream{Elems: []*proto.ProtoTopSumElement{elem}, PositiveLen: POINTER_ONE_UINT32}}}
}

func (downOp DownstreamTopSSub) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	elem := protobuf.GetTopsumOp().GetElems()[0]
	downOp.Id, downOp.Score, downOp.Data, downOp.replicate = elem.GetId(), -elem.GetScore(), emptyData, new(bool)
	if elem.Data != nil {
		downOp.Data = tools.NewByteSlicePtr(elem.Data)
	}
	return downOp
}

func (downOp DownstreamTopSSub) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	elem := &proto.ProtoTopSumElement{Id: pb.Int32(downOp.Id), Score: pb.Int32(-downOp.Score)}
	if downOp.Data != nil && len(*downOp.Data) > 0 {
		elem.Data = *downOp.Data
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopsumOp{TopsumOp: &proto.ProtoTopSumDownstream{Elems: []*proto.ProtoTopSumElement{elem}, PositiveLen: POINTER_ZERO_UINT32}}}
}

func (downOp DownstreamTopSAddAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	topSumProto := protobuf.GetTopsumOp()
	elemsProto := topSumProto.GetElems()
	scores := make([]TopKScore, len(elemsProto))
	for i, elem := range elemsProto {
		currScore := TopKScore{Id: elem.GetId(), Score: elem.GetScore(), Data: emptyData}
		if elem.Data != nil {
			currScore.Data = tools.NewByteSlicePtr(elem.Data)
		}
		scores[i] = currScore
	}
	downOp.Scores, downOp.srcReplicaID = &scores, uint16(topSumProto.GetReplicaID())
	return downOp
}

func (downOp DownstreamTopSAddAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	scores := *downOp.Scores
	if len(scores) > 0 && scores[len(scores)-1].Id == SPECIAL_SCORE.Id { //Must be serialized as a DownstreamTopSAddAndSubAll (i.e., contains both positive and negative scores).
		specialScore := scores[len(scores)-1]
		fmt.Printf("[TopSum][DownstreamTopSAddAll]Possible special score found: %+v. Confirming...\n", specialScore)
		if specialScore.Data != nil {
			data := *specialScore.Data
			fmt.Printf("[TopSum][DownstreamTopSAddAll]Possible special score found: %+v. Data not null. Confirming...\n", specialScore)
			if len(data) == 1 && data[0] == SPECIAL_SCORE_DATA { //Confirmed, this is indeed the signaling that this should be a DownstreamTopSAddAndSubAll
				nPositive := specialScore.Score
				adds, subs := scores[:nPositive], scores[nPositive:len(scores)-1]
				fmt.Printf("[TopSum][DownstreamTopSAddAll]Special score found: %+v. Issuing DownstreamTOpSAddAndSubAll with %d and %d entries\n",
					specialScore, len(adds), len(subs))
				return DownstreamTopSAddAndSubAll{AddScores: &adds, SubScores: &subs, srcReplicaID: downOp.srcReplicaID}.ToReplicatorObj()
			}
		}
	} //else: it's a normal DownstreamTopSAddAll
	protoElems := make([]*proto.ProtoTopSumElement, len(scores))
	for i, score := range scores {
		//fmt.Printf("[TopSum][DownstreamTopSAddAll]Score: %+v\n", score)
		curr := proto.ProtoTopSumElement{Id: pb.Int32(score.Id), Score: pb.Int32(score.Score)}
		if score.Data != nil && len(*score.Data) > 0 {
			curr.Data = *score.Data
		}
		protoElems[i] = &curr
	}
	//fmt.Println()
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopsumOp{TopsumOp: &proto.ProtoTopSumDownstream{Elems: protoElems, PositiveLen: pb.Uint32(uint32(len(scores))), ReplicaID: pb.Int32(int32(downOp.srcReplicaID))}}}
}

func (downOp DownstreamTopSSubAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	elemsProto := protobuf.GetTopsumOp().GetElems()
	scores := make([]TopKScore, len(elemsProto))
	for i, elem := range elemsProto {
		currScore := TopKScore{Id: elem.GetId(), Score: -elem.GetScore(), Data: emptyData}
		if elem.Data != nil {
			currScore.Data = tools.NewByteSlicePtr(elem.Data)
		}
		scores[i] = currScore
	}
	downOp.Scores = &scores
	return downOp
}

func (downOp DownstreamTopSSubAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	scores := *downOp.Scores
	protoElems := make([]*proto.ProtoTopSumElement, len(scores))
	for i, score := range scores {
		curr := proto.ProtoTopSumElement{Id: pb.Int32(score.Id), Score: pb.Int32(-score.Score)}
		if score.Data != nil && len(*score.Data) > 0 {
			curr.Data = *score.Data
		}
		protoElems[i] = &curr
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopsumOp{TopsumOp: &proto.ProtoTopSumDownstream{Elems: protoElems, PositiveLen: POINTER_ZERO_UINT32}}}
}

func (downOp DownstreamTopSAddAndSubAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	topSumProto := protobuf.GetTopsumOp()
	elemsProto, nPositive := topSumProto.GetElems(), int(topSumProto.GetPositiveLen())
	addScores, subScores := make([]TopKScore, nPositive), make([]TopKScore, len(elemsProto)-nPositive)
	var currElem *proto.ProtoTopSumElement
	for i := 0; i < nPositive; i++ {
		currElem = elemsProto[i]
		currScore := TopKScore{Id: currElem.GetId(), Score: currElem.GetScore(), Data: emptyData}
		if currElem.Data != nil {
			currScore.Data = tools.NewByteSlicePtr(currElem.Data)
		}
		addScores[i] = currScore
		/*if currScore.Score < 0 {
			fmt.Printf("[TopSAddAndSubAll][FromReplicatorObj]Warning: element %+v, index %d, of adds is negative! NElems (add/sub/total): %d/%d/%d. Scores: %+v\n",
				currScore, i, len(addScores), len(subScores), len(elemsProto), elemsProto)
		}*/
	}
	for i := nPositive; i < len(elemsProto); i++ {
		currElem = elemsProto[i]
		currScore := TopKScore{Id: currElem.GetId(), Score: -currElem.GetScore(), Data: emptyData}
		if currElem.Data != nil {
			currScore.Data = tools.NewByteSlicePtr(currElem.Data)
		}
		subScores[i-nPositive] = currScore
		/*if currScore.Score < 0 {
			fmt.Printf("[TopSAddAndSubAll][FromReplicatorObj]Warning: element %+v, index %d, of subs is negative! NElems (add/sub/total): %d/%d/%d. Scores: %+v\n",
				currScore, i, len(addScores), len(subScores), len(elemsProto), elemsProto)
		}*/
	}
	downOp.AddScores, downOp.SubScores, downOp.srcReplicaID = &addScores, &subScores, uint16(topSumProto.GetReplicaID())
	return downOp
}

func (downOp DownstreamTopSAddAndSubAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	addScores, subScores := *downOp.AddScores, *downOp.SubScores
	protoElems := make([]*proto.ProtoTopSumElement, len(addScores)+len(subScores))
	for i, score := range addScores {
		curr := proto.ProtoTopSumElement{Id: pb.Int32(score.Id), Score: pb.Int32(score.Score)}
		if score.Data != nil && len(*score.Data) > 0 {
			curr.Data = *score.Data
		}
		protoElems[i] = &curr
		/*if score.Score < 0 {
			fmt.Printf("[TopSAddAndSubAll][ToReplicatorObj]Warning: element %+v, index %d, of adds is negative! NElems (add/sub/total): %d/%d/%d.\n",
				score, i, len(addScores), len(subScores), len(protoElems))
		}*/
	}
	offset := len(addScores)
	for i, score := range subScores {
		curr := proto.ProtoTopSumElement{Id: pb.Int32(score.Id), Score: pb.Int32(-score.Score)}
		if score.Data != nil && len(*score.Data) > 0 {
			curr.Data = *score.Data
		}
		protoElems[i+offset] = &curr
		/*if score.Score < 0 {
			fmt.Printf("[TopSAddAndSubAll][ToReplicatorObj]Warning: element %+v, index %d, of subs is negative! NElems (add/sub/total): %d/%d/%d.\n",
				score, i, len(addScores), len(subScores), len(protoElems))
		}*/
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopsumOp{TopsumOp: &proto.ProtoTopSumDownstream{Elems: protoElems, PositiveLen: pb.Uint32(uint32(offset)), ReplicaID: pb.Int32(int32(downOp.srcReplicaID))}}}
}

func (downOp TopSInit) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return TopSInit(protobuf.GetTopkinitOp().GetTopSize())
}

func (downOp TopSInit) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkinitOp{TopkinitOp: &proto.ProtoTopKInitDownstream{TopSize: pb.Uint32(uint32(downOp)), TopType: proto.CRDTType_TOPSUM.Enum()}}}
}

//Uses same queries and states as TopKRmv.

func (crdt *TopSumCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	protoElems, protoNotTop := make([]*proto.ProtoTopSumElement, len(crdt.elems)),
		make([]*proto.ProtoTopSumElement, len(crdt.elems))
	i, j := 0, 0

	for _, elem := range crdt.elems {
		protoElems[i] = &proto.ProtoTopSumElement{Id: &elem.Id, Score: &elem.Score, Data: *elem.Data}
		i++
	}
	for _, elem := range crdt.notInTop {
		notProp, has := crdt.notInTop[elem.Id]
		modScore := elem.Score
		if has {
			modScore -= notProp.Score
		}
		protoElems[j] = &proto.ProtoTopSumElement{Id: &elem.Id, Score: &modScore, Data: *elem.Data}
		j++
	}

	/*var smallest *proto.ProtoTopSumElement = nil
	if crdt.smallestScore != nil {
		smallest = &proto.ProtoTopSumElement{Id: &crdt.smallestScore.Id, Score: &crdt.smallestScore.Score,
			Data: *crdt.smallestScore.Data}
	}*/
	return &proto.ProtoState{State: &proto.ProtoState_Topsum{Topsum: &proto.ProtoTopSumState{Elems: protoElems, NotTop: protoNotTop,
		/*Smallest: smallest,*/ MaxElems: pb.Int32(int32(crdt.maxElems))}}}
}

func (crdt *TopSumCrdt) FromProtoState(proto *proto.ProtoState, ts clocksi.Timestamp, replicaID uint16) (newCRDT CRDT) {
	topSumProto := proto.GetTopsum()
	elems, notTop, notProp := make(map[int32]*TopKScore), make(map[int32]*TopKScore), make(map[int32]*TopKScore)

	for _, protoElem := range topSumProto.GetElems() {
		data := protoElem.GetData()
		elems[protoElem.GetId()] = &TopKScore{Id: protoElem.GetId(), Score: protoElem.GetScore(), Data: &data}
	}
	for _, protoElem := range topSumProto.GetNotTop() {
		data := protoElem.GetData()
		notTop[protoElem.GetId()] = &TopKScore{Id: protoElem.GetId(), Score: protoElem.GetScore(), Data: &data}
	}

	/*smallestProto := topSumProto.GetSmallest()
	smallestData := smallestProto.GetData()
	smallestScore := &TopKScore{Id: smallestProto.GetId(), Score: smallestProto.GetScore(), Data: &smallestData}*/

	topSum := (&TopSumCrdt{elems: elems, notInTop: notTop, notPropagated: notProp, smallestScores: newMinBuffer(minBufferSize, MIN_SCORE), /*smallestScore: smallestScore,*/
		highestNotTop: newMaxBuffer(minBufferSize, MIN_SCORE), maxElems: int(topSumProto.GetMaxElems())}).initializeFromSnapshot(ts, replicaID)
	topSum.findAndUpdateMin()
	topSum.findAndUpdateNotTopMax()
	return topSum
}

func (crdt *TopSumCrdt) GetCRDT() CRDT { return crdt }
