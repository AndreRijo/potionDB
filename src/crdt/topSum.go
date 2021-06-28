package crdt

import (
	"potionDB/src/clocksi"
	"math"
	"potionDB/src/proto"
	"sort"

	pb "github.com/golang/protobuf/proto"
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
	*genericInversibleCRDT

	//Max number of elements that can be in top-K
	maxElems int
	//The "smallest score" in the top-K. Useful to know if when a new add arrives it should be added to the top or "hidden" to notInTop
	smallestScore *TopKScore
	//Elements that are in the top-K
	elems map[int32]*TopKScore
	//Elems which sum is < smallestScore
	notInTop map[int32]*TopKScore
	//Sum of adds not yet propagated.
	notPropagated map[int32]*TopKScore
	replicaID     int16
	//Buffer for getTopN and getTopAbove.
	//Each time the top is modified it gets nilled, and is rebuilt on the first execution of one of those queries.
	//Note that changes to notInTop DOES NOT nil this buffer.
	sortedElems []TopKScore
}

type TopSValueState struct {
	Scores []TopKScore
}

type TopSAdd struct {
	TopKScore
}

type TopSAddAll struct {
	Scores []TopKScore
}

type TopSSub struct {
	TopKScore
}

type TopSSubAll struct {
	Scores []TopKScore
}

//Just passing the score is enough
//However, we need to keep a bool pointer to know whenever this operation must be replicated or not
//Reason for pointer: structs are copied by value, thus this CRDT receives a copy of the version in materializer.
type DownstreamTopSAdd struct {
	TopKScore
	replicate    *bool
	srcReplicaID int16 //If it comes from another replica, the processing may be different
}

//Note: Doesn't need replicate *bool as if there's nothing to replicate, len(Scores) == 0.
type DownstreamTopSAddAll struct {
	Scores       *[]TopKScore //Pointer to slice so that we can modify the set of scores AFTER we conclude which ones are to be replicated
	srcReplicaID int16
}

type DownstreamTopSSub struct {
	TopKScore
	replicate *bool
	//TopSSub doesn't need srcReplicaID as a decrement never triggers a new operation/processing.
}

type DownstreamTopSSubAll struct {
	Scores *[]TopKScore
}

type GetTopSumNArguments struct {
	NumberEntries int32
}

type GetTopSumAboveValueArguments struct {
	MinValue int32
}

//Note: notPropagated is ignored when rebuilding versions, as it is assumed only reads are done on old CRDTs.

//Added to top
type TopSumAddEffect struct {
	newScore TopKScore
	oldScore int32
	oldMin   TopKScore //Due to increments, the min may change.
}

//Added to notTop
type TopSumAddNotTopEffect struct {
	newScore TopKScore
	oldScore int32
}

//A new element was promoted to the top, which led to the old element being moved to nonTop.
type TopSumAddReplaceEffect struct {
	newElem         TopKScore
	newElemOldScore int32     //Might have been an increase
	oldElem         TopKScore //The element that went to notInTop was the previous min
}

//Equal to TopSumAddReplaceEffect, but the swap is instead from top to nonTop. Different processing though.
type TopSumSubReplaceEffect struct {
	newElem         TopKScore
	newElemOldScore int32     //Might have been an increase
	oldElem         TopKScore //The element that went to notInTop was the previous min
}

func (score TopKScore) copy() (newScoreP *TopKScore) {
	newScoreP = new(TopKScore)
	newScoreP.Id, newScoreP.Score, newScoreP.Data = score.Id, score.Score, score.Data
	return
}

func (score TopKScore) isHigher(other *TopKScore) bool {
	if score.Score > other.Score {
		return true
	}
	return score.Score == other.Score && score.Id > other.Score
}

func (crdt *TopSumCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

//Ops
func (args TopSAdd) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args TopSAddAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args TopSSub) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args TopSSubAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

//Downstreams
func (args DownstreamTopSAdd) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args DownstreamTopSAdd) MustReplicate() bool { return *args.replicate }

func (args DownstreamTopSAddAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args DownstreamTopSAddAll) MustReplicate() bool { return len(*args.Scores) > 0 }

func (args DownstreamTopSSub) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args DownstreamTopSSub) MustReplicate() bool { return *args.replicate }

func (args DownstreamTopSSubAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args DownstreamTopSSubAll) MustReplicate() bool { return len(*args.Scores) > 0 }

//States
func (args TopSValueState) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args TopSValueState) GetREADType() proto.READType { return proto.READType_FULL }

//Reads
func (args GetTopSumNArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args GetTopSumNArguments) GetREADType() proto.READType { return proto.READType_GET_N }

func (args GetTopSumAboveValueArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPSUM }

func (args GetTopSumAboveValueArguments) GetREADType() proto.READType {
	return proto.READType_GET_ABOVE_VALUE
}

const (
	defaultTopSSize = 100 //Default number of top positions
)

func (crdt *TopSumCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return crdt.InitializeWithSize(startTs, replicaID, defaultTopSSize)
}

func (crdt *TopSumCrdt) InitializeWithSize(startTs *clocksi.Timestamp, replicaID int16, size int) (newCrdt CRDT) {
	crdt = &TopSumCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		maxElems:              size,
		elems:                 make(map[int32]*TopKScore),
		notInTop:              make(map[int32]*TopKScore),
		notPropagated:         make(map[int32]*TopKScore),
		replicaID:             replicaID,
	}
	newCrdt = crdt
	return
}

//Used to initialize when building a CRDT from a remote snapshot
func (crdt *TopSumCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *TopSumCrdt) {
	crdt.genericInversibleCRDT = (&genericInversibleCRDT{}).initialize(startTs)
	return crdt
}

//Note: Also accepts TopK types of reads.
func (crdt *TopSumCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
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
	}
	return nil
}

func (crdt *TopSumCrdt) getState(updsNotYetApplied []*UpdateArguments) (state State) {
	values := make([]TopKScore, len(crdt.elems))
	i := 0
	for _, elem := range crdt.elems {
		values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
		i++
	}
	return TopSValueState{Scores: values}
}

//Note: in the current implementation, at most N entries are returned, even if N+1 has the same value as N.
func (crdt *TopSumCrdt) getTopN(numberEntries int32, updsNotYetApplied []*UpdateArguments) (state State) {
	if crdt.sortedElems == nil {
		//TODO: May be an issue when updsNotYetApplied get considered.
		crdt.sortedElems = crdt.getState(updsNotYetApplied).(TopSValueState).Scores
		sort.Slice(crdt.sortedElems, func(i, j int) bool { return crdt.sortedElems[i].Score > crdt.sortedElems[j].Score })
	}
	if numberEntries >= int32(len(crdt.sortedElems)) {
		//TODO: Should I include notInTop?
		return TopSValueState{Scores: crdt.sortedElems}
	}
	return TopSValueState{Scores: crdt.sortedElems[:numberEntries]}
}

func (crdt *TopSumCrdt) getTopSAboveValue(minValue int32, updsNotYetApplied []*UpdateArguments) (state State) {
	values := make([]TopKScore, len(crdt.elems))
	actuallyAdded := 0
	//Faster to do with sortedElems if it's available.
	if crdt.sortedElems != nil {
		for _, elem := range crdt.sortedElems {
			if elem.Score >= minValue {
				values[actuallyAdded] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
				actuallyAdded++
			} else {
				break
			}
		}
	} else {
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
	}
	return nil
}

func (crdt *TopSumCrdt) getTopSAddDownstreamArgs(addOp TopSAdd) (downstreamArgs DownstreamArguments) {
	if addOp.Data == nil {
		addOp.Data = &[]byte{}
	}
	return DownstreamTopSAdd{TopKScore: addOp.TopKScore, replicate: new(bool), srcReplicaID: crdt.replicaID} //new(bool): false
}

func (crdt *TopSumCrdt) getTopSAddAllDownstreamArgs(addOp TopSAddAll) (downstreamArgs DownstreamArguments) {
	for _, score := range addOp.Scores {
		if score.Data == nil {
			score.Data = &[]byte{}
		}
	}
	return DownstreamTopSAddAll{Scores: &addOp.Scores, srcReplicaID: crdt.replicaID}
}

func (crdt *TopSumCrdt) getTopSSubDownstreamArgs(subOp TopSSub) (downstreamArgs DownstreamArguments) {
	if subOp.Data == nil {
		subOp.Data = &[]byte{}
	}
	return DownstreamTopSSub{TopKScore: subOp.TopKScore, replicate: new(bool)}
}

func (crdt *TopSumCrdt) getTopSSubAllDownstreamArgs(subOp TopSSubAll) (downstreamArgs DownstreamArguments) {
	for _, score := range subOp.Scores {
		if score.Data == nil {
			score.Data = &[]byte{}
		}
	}
	return DownstreamTopSSubAll{Scores: &subOp.Scores}
}

func (crdt *TopSumCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect, otherDownstreamArgs := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)
	return
}

func (crdt *TopSumCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	switch typedArgs := downstreamArgs.(type) {
	case DownstreamTopSAdd:
		return crdt.applyTopSAddDownstreamArgs(typedArgs)
	case DownstreamTopSSub:
		return crdt.applyTopSSubDownstreamArgs(typedArgs), nil
	case DownstreamTopSAddAll:
		return crdt.applyTopSAddAllDownstreamArgs(typedArgs)
	case DownstreamTopSSubAll:
		return crdt.applyTopSSubAllDownstreamArgs(typedArgs), nil
	}
	return nil, nil
}

//TODO: I need to check pointers, as they may be shared between multiple maps (mainly, non-propagated + other two.)
func (crdt *TopSumCrdt) applyTopSAddDownstreamArgs(op DownstreamTopSAdd) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	var effectValue Effect = NoEffect{}
	//Case 1: no elements yet
	if len(crdt.elems) == 0 {
		crdt.smallestScore = op.TopKScore.copy()
		crdt.elems[op.Id] = crdt.smallestScore
		*op.replicate = true
		effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
		return &effectValue, nil
	}

	//Case 2: increase to element on top, thus gets propagated.
	entry, has := crdt.elems[op.Id]
	if has {
		//Increase
		effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: entry.Score}
		entry.Score += op.Score
		*op.replicate = true
		if crdt.smallestScore.Id == entry.Id {
			//This was the min, which is now updated
			crdt.findAndUpdateMin()
		}
		return &effectValue, nil
	} else if len(crdt.elems) < crdt.maxElems {
		//Case 3: not enough elements yet, and this element doesn't exist yet.
		copy := op.TopKScore.copy()
		crdt.elems[op.Id] = copy
		//check if it should now be min
		if crdt.smallestScore.isHigher(copy) {
			effectValue = TopSumAddEffect{newScore: op.TopKScore,
				oldMin: *crdt.smallestScore, oldScore: math.MinInt32}
			crdt.smallestScore = copy
		} else {
			effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
		}
		*op.replicate = true
		return &effectValue, nil
	}

	//Frow now on, it definitely isn't on top and top is full.
	entry, has = crdt.notInTop[op.Id]
	nonPropEntry, nonPropHas := crdt.notPropagated[op.Id]
	if has {
		//Increase
		oldScore := entry.Score
		entry.Score += op.Score
		//Case 4: wasn't on top, but now it will be.
		if entry.isHigher(crdt.smallestScore) {
			effectValue = TopSumAddReplaceEffect{newElem: *entry, newElemOldScore: oldScore, oldElem: *crdt.smallestScore}
			crdt.notInTop[crdt.smallestScore.Id] = crdt.smallestScore
			delete(crdt.elems, crdt.smallestScore.Id)
			delete(crdt.notInTop, entry.Id)
			crdt.elems[entry.Id] = entry
			crdt.findAndUpdateMin()

			if nonPropHas {
				//Include those on op
				op.Score += nonPropEntry.Score
				if len(*op.Data) == 0 {
					op.Data = nonPropEntry.Data
				}
				delete(crdt.notPropagated, entry.Id)
				if op.srcReplicaID != crdt.replicaID {
					//Remote operation, so we must force the propagation of nonPropEntry.Score
					return &effectValue, DownstreamTopSAdd{TopKScore: TopKScore{Id: op.Id, Score: nonPropEntry.Score, Data: nonPropEntry.Data},
						replicate: new(bool), srcReplicaID: crdt.replicaID}
				}
			}
			//If nonProHas = false, then this op by itself is enough to put on top (and thus, must be replicated)
			*op.replicate = true
			return &effectValue, nil
		}
		//Case 5: still not on top. Processing differs depending if this is a local or a remote operation
		if op.srcReplicaID != crdt.replicaID && nonPropEntry != nil {
			effectValue = TopSumAddNotTopEffect{newScore: *entry, oldScore: oldScore}
			//Apply rule for existing nonProp. Return right away.
			if crdt.shouldReplicate(entry.Score, nonPropEntry.Score) {
				return &effectValue, DownstreamTopSAdd{TopKScore: TopKScore{Id: op.Id, Score: nonPropEntry.Score, Data: nonPropEntry.Data},
					replicate: new(bool), srcReplicaID: crdt.replicaID}
			}
			return &effectValue, nil
		}
		//Apply rule to check if it should be replicated
		if nonPropEntry == nil {
			nonPropEntry = &op.TopKScore
			crdt.notPropagated[entry.Id] = nonPropEntry
		} else {
			nonPropEntry.Score += op.Score
		}
		if crdt.shouldReplicate(entry.Score, nonPropEntry.Score) {
			op.Score = nonPropEntry.Score
			if len(*op.Data) == 0 {
				op.Data = nonPropEntry.Data
			}
			delete(crdt.notPropagated, entry.Id)
			*op.replicate = true
		}
		effectValue = TopSumAddNotTopEffect{newScore: *entry, oldScore: oldScore}
		return &effectValue, nil
	}

	newEntry := op.TopKScore.copy()
	//Case 6: new elem that will be on top
	//In this case, it's not in elems or notOnTop. So, new entry.
	if op.TopKScore.isHigher(crdt.smallestScore) {
		effectValue = TopSumAddReplaceEffect{newElem: op.TopKScore, newElemOldScore: math.MinInt32, oldElem: *crdt.smallestScore}
		crdt.notInTop[crdt.smallestScore.Id] = crdt.smallestScore
		delete(crdt.elems, crdt.smallestScore.Id)
		crdt.elems[newEntry.Id] = newEntry
		crdt.findAndUpdateMin()
		*op.replicate = true
		return &effectValue, nil
	}

	crdt.notInTop[newEntry.Id] = newEntry
	//Case 7: new elem that isn't on top. It may still, however, need to be replicated.
	if crdt.shouldReplicate(0, op.Score) {
		*op.replicate = true
		effectValue = TopSumAddNotTopEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
		return &effectValue, nil
	}
	//Case 8: new elem, and add is too small to be worth replicating.
	crdt.notPropagated[newEntry.Id] = newEntry.copy()
	effectValue = TopSumAddNotTopEffect{newScore: op.TopKScore, oldScore: math.MinInt32}

	return &effectValue, nil
}

func (crdt *TopSumCrdt) applyTopSAddAllDownstreamArgs(op DownstreamTopSAddAll) (effect *Effect, otherDownstreamArgs DownstreamArguments) {
	//TODO: Effects
	var effectValue Effect = NoEffect{}
	scores := *op.Scores
	downScores := make([]TopKScore, len(scores))
	var otherDownScores []TopKScore
	if crdt.replicaID != op.srcReplicaID {
		//May need to generate some elements for otherDownstreamArgs
		otherDownScores = make([]TopKScore, 0, 10) //Will just use append
	}
	downI := 0
	var currScore TopKScore
	if len(scores) == 0 {
		return &effectValue, nil
	}
	i := 0
	//Case 1: no elements yet
	if len(crdt.elems) == 0 {
		currScore = scores[0]
		i++
		crdt.smallestScore = currScore.copy()
		crdt.elems[currScore.Id] = crdt.smallestScore
		downScores[downI], downI = currScore, downI+1
	}
	var entry, nonPropEntry, copy *TopKScore
	var has, nonPropHas bool
	for ; i < len(scores); i++ {
		currScore = scores[i]
		entry, has = crdt.elems[currScore.Id]
		if has {
			//Case 2: increase to element on top
			entry.Score += currScore.Score
			if crdt.smallestScore.Id == entry.Id {
				//This was the min, which is now updated
				crdt.findAndUpdateMin()
			}
			downScores[downI], downI = currScore, downI+1
		} else if len(crdt.elems) < crdt.maxElems {
			//Case 3: not enough elements yet, and this element doesn't exist yet.
			copy = currScore.copy()
			crdt.elems[currScore.Id] = copy
			//check if it should now be min
			if crdt.smallestScore.isHigher(copy) {
				crdt.smallestScore = copy
			}
			downScores[downI], downI = currScore, downI+1
		} else {
			//Not on top for sure & top is full
			entry, has = crdt.notInTop[currScore.Id]
			nonPropEntry, nonPropHas = crdt.notPropagated[currScore.Id]
			if has {
				entry.Score += currScore.Score
				//Case 4: wasn't on top, but not it will be
				if entry.isHigher(crdt.smallestScore) {
					crdt.notInTop[crdt.smallestScore.Id] = crdt.smallestScore
					delete(crdt.elems, crdt.smallestScore.Id)
					delete(crdt.notInTop, entry.Id)
					crdt.elems[entry.Id] = entry
					crdt.findAndUpdateMin()

					if nonPropHas {
						//Include those on op
						currScore.Score += nonPropEntry.Score
						if len(*currScore.Data) == 0 {
							currScore.Data = nonPropEntry.Data
						}
						delete(crdt.notPropagated, entry.Id)
						if op.srcReplicaID != crdt.replicaID {
							//Remote operation, so we must force the propagation of nonPropEntry.Score
							otherDownScores = append(otherDownScores, TopKScore{Id: currScore.Id, Score: nonPropEntry.Score, Data: nonPropEntry.Data})
						}
					}
					//Case 5 (next): still not on top. Processing differs depending if this is a local or a remote operation
				} else if op.srcReplicaID != crdt.replicaID && nonPropEntry != nil {
					if crdt.shouldReplicate(entry.Score, nonPropEntry.Score) {
						otherDownScores = append(otherDownScores, TopKScore{Id: currScore.Id, Score: nonPropEntry.Score, Data: nonPropEntry.Data})
					}
				} else {
					//Apply rule to check if it should be replicated
					if nonPropEntry == nil {
						nonPropEntry = &currScore
						crdt.notPropagated[entry.Id] = nonPropEntry
					} else {
						nonPropEntry.Score += currScore.Score
					}
					if crdt.shouldReplicate(entry.Score, nonPropEntry.Score) {
						currScore.Score = nonPropEntry.Score
						if len(*currScore.Data) == 0 {
							currScore.Data = nonPropEntry.Data
						}
						delete(crdt.notPropagated, entry.Id)
						downScores[downI], downI = currScore, downI+1
					}
				}
			} else {
				copy = currScore.copy()
				//Element doesn't exist in either top or notInTop.
				//Case 6: new elem that will be on top
				if currScore.isHigher(crdt.smallestScore) {
					crdt.notInTop[crdt.smallestScore.Id] = crdt.smallestScore
					delete(crdt.elems, crdt.smallestScore.Id)
					crdt.elems[copy.Id] = copy
					crdt.findAndUpdateMin()
					downScores[downI], downI = currScore, downI+1
				} else if crdt.shouldReplicate(0, currScore.Score) {
					//Case 7: new elem that isn't on top but needs to be replicated
					crdt.notInTop[crdt.smallestScore.Id] = copy
					downScores[downI], downI = currScore, downI+1
				} else {
					//Case 8: new elem that isn't top and it's not high enough to be replicated
					crdt.notInTop[copy.Id], crdt.notPropagated[copy.Id] = copy, copy.copy()
				}
			}
		}
	}
	if downI > 0 {
		downScores = downScores[:downI]
	}
	*op.Scores = downScores
	if len(otherDownScores) > 0 {
		return &effectValue, DownstreamTopSAddAll{Scores: &otherDownScores, srcReplicaID: crdt.replicaID}
	}
	return &effectValue, nil
}

func (crdt *TopSumCrdt) applyTopSSubDownstreamArgs(op DownstreamTopSSub) (effect *Effect) {
	//Note: the score in the operation is always negative (hence why += is used)
	var effectValue Effect = NoEffect{}
	//TODO: effects
	//Case 1: no elements yet
	if len(crdt.elems) == 0 {
		crdt.smallestScore = op.TopKScore.copy()
		crdt.elems[op.Id] = crdt.smallestScore
		*op.replicate = true
		//new top element
		effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
		return &effectValue
	}

	//Case 2: decrease to element in top, thus it must be propagated
	entry, has := crdt.elems[op.Id]
	if has {
		oldScore := entry.Score
		entry.Score += op.Score
		if crdt.smallestScore.isHigher(entry) {
			if len(crdt.notInTop) == 0 {
				effectValue = TopSumAddEffect{newScore: *entry, oldScore: oldScore,
					oldMin: *crdt.smallestScore}
				//Special case, no move but it becomes the smallest score
				crdt.smallestScore = entry
			} else {
				//Move to notTop, find new min, add that min to top.
				delete(crdt.elems, op.Id)
				crdt.notInTop[op.Id] = entry
				crdt.findAndUpdateMin()
				newMin := crdt.smallestScore
				crdt.elems[newMin.Id] = newMin
				//replace
				effectValue = TopSumSubReplaceEffect{newElem: *entry, oldElem: *newMin, newElemOldScore: oldScore}
			}
		}
		//Nothing do on else (element stays in top)
		*op.replicate = true
		return &effectValue
	} else if len(crdt.elems) < crdt.maxElems {
		//Case 3: has space on top, and element doesn't yet exist
		copy := op.TopKScore.copy()
		crdt.elems[op.Id] = copy
		if crdt.smallestScore.isHigher(copy) {
			effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32, oldMin: *crdt.smallestScore}
			crdt.smallestScore = copy
		} else {
			effectValue = TopSumAddEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
		}
		*op.replicate = true
		//new top, may update min
		return &effectValue
	}

	//Top is full and the element isn't there
	entry, has = crdt.notInTop[op.Id]
	nonPropEntry, nonPropHas := crdt.notPropagated[op.Id]
	if has {
		//No need to propagate, storing the decrement is enough. It might be relevant if the element ever gets to the top.
		//Case 4: element wasn't on top, so it won't be now for sure
		entry.Score += op.Score
		if nonPropHas {
			nonPropEntry.Score += op.Score
		} else {
			crdt.notPropagated[op.Id] = op.copy()
		}
		//Decrement to non-top
		return &effectValue
	}

	newEntry := op.TopKScore.copy()
	//Case 5: element doesn't exist. It may need to go for top
	if op.TopKScore.isHigher(crdt.smallestScore) {
		//top. Must propagate.
		crdt.elems[op.Id] = newEntry
		delete(crdt.elems, crdt.smallestScore.Id)
		//TopSumAddReplace because this element is being added to top;
		effectValue = TopSumAddReplaceEffect{newElem: op.TopKScore, newElemOldScore: math.MinInt32, oldElem: *crdt.smallestScore}
		crdt.notInTop[op.Id] = crdt.smallestScore
		crdt.findAndUpdateMin()
		*op.replicate = true
		return &effectValue
	}

	//not top. Don't propagate.
	crdt.notInTop[op.Id] = newEntry
	crdt.notPropagated[op.Id] = newEntry.copy()
	//New element to non-top
	effectValue = TopSumAddNotTopEffect{newScore: op.TopKScore, oldScore: math.MinInt32}
	return &effectValue
}

func (crdt *TopSumCrdt) applyTopSSubAllDownstreamArgs(op DownstreamTopSSubAll) (effect *Effect) {
	//TODO: Effects
	var effectValue Effect = NoEffect{}
	scores := *op.Scores
	downScores := make([]TopKScore, len(scores))
	downI := 0
	var currScore TopKScore
	if len(scores) == 0 {
		return &effectValue
	}
	i := 0
	//Case 1: no elements yet
	if len(crdt.elems) == 0 {
		currScore = scores[0]
		i++
		crdt.smallestScore = currScore.copy()
		crdt.elems[currScore.Id] = crdt.smallestScore
		downScores[downI], downI = currScore, downI+1
	}
	var entry, nonPropEntry, copy *TopKScore
	var has, nonPropHas bool
	for ; i < len(scores); i++ {
		currScore = scores[i]
		entry, has = crdt.elems[currScore.Id]
		if has {
			//Case 2: decrease to element in top, thus it must be propagated
			entry.Score += currScore.Score
			if crdt.smallestScore.isHigher(entry) {
				if len(crdt.notInTop) == 0 {
					//Special case, no move but it becomes the smallest score
					crdt.smallestScore = entry
				}
			} else {
				//Move to notTop, find new min, add that min to top
				delete(crdt.elems, entry.Id)
				crdt.notInTop[entry.Id] = entry
				crdt.findAndUpdateMin()
				crdt.elems[crdt.smallestScore.Id] = crdt.smallestScore
			}
			downScores[downI], downI = currScore, downI+1
		} else if len(crdt.elems) < crdt.maxElems {
			//Case 3: has space on top, and element doesn't yet exist
			copy = entry.copy()
			crdt.elems[entry.Id] = copy
			if crdt.smallestScore.isHigher(copy) {
				crdt.smallestScore = copy
			}
			downScores[downI], downI = currScore, downI+1
		} else {
			//Top is full and element isn't there
			entry, has = crdt.notInTop[entry.Id]
			nonPropEntry, nonPropHas = crdt.notPropagated[entry.Id]
			if has {
				//No need to propagate, storing the decrement is enough. It might be relevant if the element ever gets to the top.
				//Case 4: element wasn't on top, so it won't be now for sure
				entry.Score += currScore.Score
				if nonPropHas {
					nonPropEntry.Score += currScore.Score
				} else {
					crdt.notPropagated[currScore.Id] = currScore.copy()
				}
			} else if entry.isHigher(crdt.smallestScore) {
				//Case 5: element doesn't exist, but needs to go to top
				crdt.elems[currScore.Id] = currScore.copy()
				delete(crdt.elems, crdt.smallestScore.Id)
				crdt.notInTop[currScore.Id] = crdt.smallestScore
				crdt.findAndUpdateMin()
				downScores[downI], downI = currScore, downI+1
			} else {
				//Not top. Also don't propagate.
				crdt.notInTop[currScore.Id] = currScore.copy()
				crdt.notPropagated[currScore.Id] = currScore.copy()
			}
		}
	}
	if downI > 0 {
		downScores = downScores[:downI]
	}
	*op.Scores = downScores
	return &effectValue
}

func (crdt *TopSumCrdt) shouldReplicate(existing int32, notReplicated int32) bool {
	//If every replica added as much as us, they it would be added to the top
	return notReplicated*NReplicas > crdt.smallestScore.Score-existing
}

func (crdt *TopSumCrdt) findAndUpdateMin() {
	minSoFar := TopKScore{Id: math.MaxInt32, Score: math.MaxInt32}
	for _, elem := range crdt.elems {
		if minSoFar.isHigher(elem) {
			minSoFar = *elem
		}
	}
	crdt.smallestScore = &minSoFar
}

func (crdt *TopSumCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

//METHODS FOR INVERSIBLE CRDT

func (crdt *TopSumCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := TopSumCrdt{
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		maxElems:              crdt.maxElems,
		smallestScore:         crdt.smallestScore,
		elems:                 make(map[int32]*TopKScore),
		notInTop:              make(map[int32]*TopKScore),
		notPropagated:         make(map[int32]*TopKScore),
		replicaID:             crdt.replicaID,
	}

	for id, elem := range crdt.elems {
		newCrdt.elems[id] = elem
	}
	for id, elem := range crdt.notInTop {
		newCrdt.notInTop[id] = elem
	}
	for id, elem := range crdt.notPropagated {
		newCrdt.notPropagated[id] = elem
	}

	return &newCrdt
}

func (crdt *TopSumCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
}

func (crdt *TopSumCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	effect, _ = crdt.applyDownstream(updArgs)
	return
}

func (crdt *TopSumCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case TopSumAddEffect:
		crdt.undoAddEffect(&typedEffect)
	case TopSumAddNotTopEffect:
		crdt.undoAddNotTopEffect(&typedEffect)
	case TopSumAddReplaceEffect:
		crdt.undoAddReplaceEffect(&typedEffect)
	case TopSumSubReplaceEffect:
		crdt.undoSubReplaceEffect(&typedEffect)
	}
}

func (crdt *TopSumCrdt) undoAddEffect(effect *TopSumAddEffect) {
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

func (crdt *TopSumCrdt) undoAddNotTopEffect(effect *TopSumAddNotTopEffect) {
	if effect.oldScore == math.MinInt32 {
		//Element didn't exist before
		delete(crdt.elems, effect.newScore.Id)
	} else {
		crdt.notInTop[effect.newScore.Id].Score = effect.oldScore
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

func (crdt *TopSumCrdt) undoSubReplaceEffect(effect *TopSumSubReplaceEffect) {
	//Only one case
	delete(crdt.elems, effect.newElem.Id)
}

func (crdt *TopSumCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf function
func (crdtOp TopSAdd) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	add := protobuf.GetTopkrmvop().GetAdds()[0]
	crdtOp.TopKScore = TopKScore{Id: add.GetPlayerId(), Score: add.GetScore(), Data: &add.Data}
	return crdtOp
}

func (crdtOp TopSAdd) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	add := proto.ApbIntPair{PlayerId: pb.Int32(crdtOp.Id), Score: pb.Int32(crdtOp.Score)}
	if crdtOp.Data != nil {
		add.Data = *crdtOp.Data
	}
	return &proto.ApbUpdateOperation{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: []*proto.ApbIntPair{&add}}}
}

func (crdtOp TopSSub) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	sub := protobuf.GetTopkrmvop().GetAdds()[0]
	crdtOp.TopKScore = TopKScore{Id: sub.GetPlayerId(), Score: sub.GetScore(), Data: &sub.Data}
	return crdtOp
}

func (crdtOp TopSSub) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	sub := proto.ApbIntPair{PlayerId: pb.Int32(crdtOp.Id), Score: pb.Int32(crdtOp.Score)}
	if crdtOp.Data != nil {
		sub.Data = *crdtOp.Data
	}
	return &proto.ApbUpdateOperation{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: []*proto.ApbIntPair{&sub}}}
}

func (crdtOp TopSAddAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	adds := protobuf.GetTopkrmvop().GetAdds()
	crdtOp.Scores = make([]TopKScore, len(adds))
	for i, add := range adds {
		crdtOp.Scores[i] = TopKScore{Id: add.GetPlayerId(), Score: add.GetScore(), Data: &add.Data}
	}
	return crdtOp
}

func (crdtOp TopSAddAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	protoAdds := make([]*proto.ApbIntPair, len(crdtOp.Scores))
	for i, score := range crdtOp.Scores {
		add := proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(score.Score)}
		if score.Data != nil {
			add.Data = *score.Data
		}
		protoAdds[i] = &add
	}
	return &proto.ApbUpdateOperation{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: protoAdds}}
}

func (crdtOp TopSSubAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	subs := protobuf.GetTopkrmvop().GetAdds()
	crdtOp.Scores = make([]TopKScore, len(subs))
	for i, sub := range subs {
		crdtOp.Scores[i] = TopKScore{Id: sub.GetPlayerId(), Score: sub.GetScore(), Data: &sub.Data}
	}
	return crdtOp
}

func (crdtOp TopSSubAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	protoSubs := make([]*proto.ApbIntPair, len(crdtOp.Scores))
	for i, score := range crdtOp.Scores {
		sub := proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(score.Score)}
		if score.Data != nil {
			sub.Data = *score.Data
		}
		protoSubs[i] = &sub
	}
	return &proto.ApbUpdateOperation{Topkrmvop: &proto.ApbTopkRmvUpdate{Adds: protoSubs}}
}

//Same as for TopK
func (crdtState TopSValueState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	protoScores := protobuf.GetTopk().GetValues()
	if protoScores == nil {
		//Partial read
		protoScores = protobuf.GetPartread().GetTopk().GetPairs().GetValues()
	}
	crdtState.Scores = make([]TopKScore, len(protoScores))
	for i, pair := range protoScores {
		data := pair.GetData()
		crdtState.Scores[i] = TopKScore{Id: pair.GetPlayerId(), Score: pair.GetScore(), Data: &data}
	}
	return crdtState
}

//Same as for TopK
func (crdtState TopSValueState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	protos := make([]*proto.ApbIntPair, len(crdtState.Scores))
	for i, score := range crdtState.Scores {
		protos[i] = &proto.ApbIntPair{PlayerId: pb.Int32(score.Id), Score: pb.Int32(score.Score), Data: *score.Data}
	}
	return &proto.ApbReadObjectResp{Topk: &proto.ApbGetTopkResp{Values: protos}}
}

func (downOp DownstreamTopSAdd) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	elem := protobuf.GetTopsumOp().GetElems()[0]
	downOp.Id, downOp.Score, downOp.Data, downOp.replicate, downOp.srcReplicaID = elem.GetId(), elem.GetScore(), &elem.Data, new(bool), math.MinInt16
	return downOp
}

func (downOp DownstreamTopSAdd) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	positive := true
	return &proto.ProtoOpDownstream{TopsumOp: &proto.ProtoTopSumDownstream{Elems: []*proto.ProtoTopSumElement{
		&proto.ProtoTopSumElement{Id: pb.Int32(downOp.Id), Score: pb.Int32(downOp.Score), Data: *downOp.Data}}, IsPositive: &positive,
	}}
}

func (downOp DownstreamTopSSub) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	elem := protobuf.GetTopsumOp().GetElems()[0]
	downOp.Id, downOp.Score, downOp.Data, downOp.replicate = elem.GetId(), elem.GetScore(), &elem.Data, new(bool)
	return downOp
}

func (downOp DownstreamTopSSub) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{TopsumOp: &proto.ProtoTopSumDownstream{Elems: []*proto.ProtoTopSumElement{
		&proto.ProtoTopSumElement{Id: pb.Int32(downOp.Id), Score: pb.Int32(downOp.Score), Data: *downOp.Data}}, IsPositive: new(bool),
	}}
}

func (downOp DownstreamTopSAddAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	elemsProto := protobuf.GetTopsumOp().GetElems()
	scores := make([]TopKScore, len(elemsProto))
	for i, elem := range elemsProto {
		scores[i] = TopKScore{Id: elem.GetId(), Score: elem.GetScore(), Data: &elem.Data}
	}
	downOp.Scores = &scores
	return downOp
}

func (downOp DownstreamTopSAddAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	scores, positive := *downOp.Scores, true
	protoElems := make([]*proto.ProtoTopSumElement, len(scores))
	for i, score := range scores {
		protoElems[i] = &proto.ProtoTopSumElement{Id: pb.Int32(score.Id), Score: pb.Int32(score.Score), Data: *score.Data}
	}
	return &proto.ProtoOpDownstream{TopsumOp: &proto.ProtoTopSumDownstream{Elems: protoElems, IsPositive: &positive}}
}

func (downOp DownstreamTopSSubAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	elemsProto := protobuf.GetTopsumOp().GetElems()
	scores := make([]TopKScore, len(elemsProto))
	for i, elem := range elemsProto {
		scores[i] = TopKScore{Id: elem.GetId(), Score: elem.GetScore(), Data: &elem.Data}
	}
	downOp.Scores = &scores
	return downOp
}

func (downOp DownstreamTopSSubAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	scores := *downOp.Scores
	protoElems := make([]*proto.ProtoTopSumElement, len(scores))
	for i, score := range scores {
		protoElems[i] = &proto.ProtoTopSumElement{Id: pb.Int32(score.Id), Score: pb.Int32(score.Score), Data: *score.Data}
	}
	return &proto.ProtoOpDownstream{TopsumOp: &proto.ProtoTopSumDownstream{Elems: protoElems, IsPositive: new(bool)}}
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

	var smallest *proto.ProtoTopSumElement = nil
	if crdt.smallestScore != nil {
		smallest = &proto.ProtoTopSumElement{Id: &crdt.smallestScore.Id, Score: &crdt.smallestScore.Score,
			Data: *crdt.smallestScore.Data}
	}
	return &proto.ProtoState{Topsum: &proto.ProtoTopSumState{Elems: protoElems, NotTop: protoNotTop,
		Smallest: smallest, MaxElems: pb.Int32(int32(crdt.maxElems))}}
}

func (crdt *TopSumCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
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

	smallestProto := topSumProto.GetSmallest()
	smallestData := smallestProto.GetData()
	smallestScore := &TopKScore{Id: smallestProto.GetId(), Score: smallestProto.GetScore(), Data: &smallestData}

	return (&TopSumCrdt{elems: elems, notInTop: notTop, notPropagated: notProp, smallestScore: smallestScore,
		maxElems: int(topSumProto.GetMaxElems())}).initializeFromSnapshot(ts, replicaID)
}
