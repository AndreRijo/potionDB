package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"sort"

	//pb "github.com/golang/protobuf/proto"
	tools "github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

//A TopK without support for removes.
//Check TopKRmv for a top with support for removes and TopSum for a top with support for incs and decs.
//Use this TopK whenever removes are not required, as not supporting removes
//Allows this TopK to store less entries and has less complex update processing.

type TopKCrdt struct {
	CRDTVM
	//Metrics to decide if we should cache a read result or not. TopN is always cached as it requires computing the sorted set.
	//Note: only updates that modify the top-K (i.e., that modify elems) count, as only those can invalidate the cache.
	nReads, nUpds int32

	//Max number of elements that can be in top-K
	maxElems int
	//The "smallest score" in the top-K. Useful to know if when a new add arrives it should be added to the top
	smallestScore TopKScore
	//Elements that are in the top-K
	elems map[int32]TopKScore

	//Buffer for getTopN and getTopAbove.
	//Each time the top is *actually* modified it gets nilled, and is rebuilt on the first execution of one of those queries.
	//Any add that does not change the top does not nill this buffer.
	sortedElems []TopKScore

	//If true, all entries whose score matches smallestScore are kept and returned.
	//Ideally, later this should be a CRDT operation or configuration
	keepTiedEntries bool
	replicaID       uint16
	tiedElems       map[int32]TopKScore
}

type DownstreamSimpleTopKAdd struct {
	TopKScore
	ToReplicate *bool
}

type DownstreamSimpleTopKAddAll struct {
	DownstreamAdds []TopKScore
}

// Effect of an TopKAdd that adds the element to the top
type TopKAddEffect struct {
	TopKScore
}

// Effect of an TopKAdd which either replaces an old value or changes the min (or both)
type TopKReplaceEffect struct {
	newElem, oldElem, oldMin TopKScore
}

type TopKReplaceAndCleanTiedEffect struct {
	newElem, oldElem, oldMin TopKScore
	oldTied                  map[int32]TopKScore
}

type TopKReplaceAndTiedEffect struct {
	newElem, oldElem, oldMin TopKScore
}

// List of TopKAddEffect and TopKReplaceEffect
type TopKAddAllEffect struct {
	effects []Effect
}

type TopKInit uint32

func (crdt *TopKCrdt) GetCRDTType() proto.CRDTType                  { return proto.CRDTType_TOPK }
func (crdt *TopKCrdt) GetDATAType() proto.DATAType                  { return proto.DATAType_DEFAULT }
func (args DownstreamSimpleTopKAdd) GetCRDTType() proto.CRDTType    { return proto.CRDTType_TOPK }
func (args DownstreamSimpleTopKAddAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK }
func (args DownstreamSimpleTopKAdd) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }
func (args DownstreamSimpleTopKAddAll) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args DownstreamSimpleTopKAdd) MustReplicate() bool            { return true }
func (args DownstreamSimpleTopKAddAll) MustReplicate() bool         { return true }
func (args TopKInit) GetCRDTType() proto.CRDTType                   { return proto.CRDTType_TOPK }
func (args TopKInit) GetDATAType() proto.DATAType                   { return proto.DATAType_DEFAULT }
func (args TopKInit) MustReplicate() bool                           { return true }

// Returns true if score (this) is higher than other (argument).
func (score TopKScore) isHigherScore(other TopKScore) bool {
	if (other == TopKScore{}) {
		return true
	}
	if score.Score > other.Score {
		return true
	}
	if score.Score == other.Score {
		return score.Id > other.Score
	}
	return false
}

// Returns true if score (this) is lower than other (argument).
func (score TopKScore) isLowerScore(other TopKScore) bool {
	if (other == TopKScore{}) {
		return true
	}
	if score.Score < other.Score {
		return true
	}
	if score.Score == other.Score {
		return score.Id < other.Id
	}
	return false
}

func (crdt *TopKCrdt) Initialize(startTs *clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	return crdt.InitializeWithSize(startTs, replicaID, defaultTopKSize)
}

func (crdt *TopKCrdt) InitializeWithSize(startTs *clocksi.Timestamp, replicaID uint16, size int) (newCrdt CRDT) {
	crdt = &TopKCrdt{
		CRDTVM:          (&genericInversibleCRDT{}).initialize(crdt),
		replicaID:       replicaID,
		maxElems:        size,
		smallestScore:   TopKScore{},
		elems:           make(map[int32]TopKScore, size),
		keepTiedEntries: true,
	}
	newCrdt = crdt
	return
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *TopKCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID uint16) (sameCRDT *TopKCrdt) {
	crdt.CRDTVM, crdt.replicaID = (&genericInversibleCRDT{}).initialize(crdt), replicaID
	return crdt
}

func (crdt *TopKCrdt) IsBigCRDT() bool { return crdt.maxElems > 100 && len(crdt.elems) > 100 }

func (crdt *TopKCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
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
		fmt.Printf("[TOPKCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *TopKCrdt) makeSortedElems() {
	var values []TopKScore
	if !crdt.keepTiedEntries {
		values = make([]TopKScore, len(crdt.elems))
	} else {
		values = make([]TopKScore, len(crdt.elems)+len(crdt.tiedElems))
	}
	i := 0
	for _, elem := range crdt.elems {
		values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
		i++
	}
	if crdt.keepTiedEntries {
		for _, tied := range crdt.tiedElems {
			values[i] = TopKScore{Id: tied.Id, Score: tied.Score, Data: tied.Data}
			i++
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Score > values[j].Score })
	crdt.sortedElems = values
}

func (crdt *TopKCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	var values []TopKScore
	if crdt.sortedElems != nil {
		values = make([]TopKScore, len(crdt.sortedElems))
		copy(values, crdt.sortedElems)
	} else {
		if !crdt.keepTiedEntries {
			values = make([]TopKScore, len(crdt.elems))
			i := 0
			for _, elem := range crdt.elems {
				values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
				i++
			}
		} else {
			values = make([]TopKScore, len(crdt.elems)+len(crdt.tiedElems))
			i := 0
			for _, elem := range crdt.elems {
				values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
				i++
			}
			for _, tied := range crdt.tiedElems {
				values[i] = TopKScore{Id: tied.Id, Score: tied.Score, Data: tied.Data}
				i++
			}
		}
	}
	return TopKValueState{Scores: values}
}

/*
Note: in the current implementation, at most N entries are returned, even if N+1 has the same value as N.
*/
func (crdt *TopKCrdt) getTopN(numberEntries int32, updsNotYetApplied []UpdateArguments) (state State) {
	if crdt.sortedElems == nil {
		//TODO: May be an issue when updsNotYetApplied get considered.
		//crdt.sortedElems = crdt.getState(updsNotYetApplied).(TopKValueState).Scores
		//sort.Slice(crdt.sortedElems, func(i, j int) bool { return crdt.sortedElems[i].Score > crdt.sortedElems[j].Score })
		crdt.makeSortedElems()
	}
	if numberEntries >= int32(len(crdt.sortedElems)) {
		return TopKValueState{Scores: crdt.sortedElems}
	}
	return TopKValueState{Scores: crdt.sortedElems[:numberEntries]}
}

func (crdt *TopKCrdt) getTopKAboveValue(minValue int32, updsNotYetApplied []UpdateArguments) (state State) {
	values := make([]TopKScore, len(crdt.elems))
	actuallyAdded := 0
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
		if minValue <= crdt.smallestScore.Score {
			values = make([]TopKScore, len(crdt.sortedElems))
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
					actuallyAdded = i
					break
				}
			}
			if actuallyAdded == 0 {
				actuallyAdded = len(crdt.elems)
			}
			if crdt.keepTiedEntries && crdt.smallestScore.Score >= minValue {
				for _, tied := range crdt.tiedElems {
					values = append(values, tied)
				}
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
		if crdt.keepTiedEntries && crdt.smallestScore.Score >= minValue {
			for _, tied := range crdt.tiedElems {
				values = append(values, tied)
			}
		}
	}

	return TopKValueState{Scores: values[:actuallyAdded]}
}

func (crdt *TopKCrdt) getTopAggregate(minValue, maxValue, bitmask int32, aggrType AggregateType, updsNotYetApplied []UpdateArguments) (state State) {
	if len(crdt.elems) == 0 {
		return getAggregateState(aggrType, 0)
	}
	if aggrType == M_MIN && crdt.smallestScore.Score > minValue { //This is already known.
		return getAggregateState(aggrType, int64(crdt.smallestScore.Score))
	}
	if crdt.sortedElems != nil {
		if aggrType == M_MAX && crdt.sortedElems[0].Score < maxValue { //This is already known.
			return getAggregateState(aggrType, int64(crdt.sortedElems[0].Score))
		}
		return aggrStrategyChooserSortedElems(crdt.sortedElems, crdt.sortedElems[0].Score, crdt.smallestScore.Score, minValue, maxValue, bitmask, aggrType)
	} else {
		return aggrStrategyChooserTopKMap(crdt.elems, crdt.smallestScore.Score, minValue, maxValue, bitmask, aggrType)
	}
}

func (crdt *TopKCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch opType := args.(type) {
	case TopKAdd:
		downstreamArgs = crdt.getTopKAddDownstreamArgs(&opType)
	case TopKAddAll:
		downstreamArgs = crdt.getTopKAddAllDownstreamArgs(&opType)
	case TopKInit:
		downstreamArgs = crdt.getInitDownstreamArgs(opType)
	case MultiUpd:
		multiDowns := make(MultiUpd, len(opType))
		for i, innerUpd := range opType {
			multiDowns[i] = crdt.Update(innerUpd)
		}
		return multiDowns
	default:
		fmt.Printf("[TopK][Update]Unknown update type: %v (%T)\n", args, args)
	}
	return
}

func (crdt *TopKCrdt) getInitDownstreamArgs(initOp TopKInit) (args TopKInit) {
	if len(crdt.elems) == 0 { //Set nElems immediately if it's the first op, in order for upcoming Update() to make correct decisions. This does not affect correctness.
		crdt.maxElems = int(initOp)
	}
	return initOp
}

func (crdt *TopKCrdt) getTopKAddDownstreamArgs(addOp *TopKAdd) (args DownstreamArguments) {
	//If at this point it will not belong to the top, it never will.
	//If it already exists with a higher score, then discard this
	//fmt.Printf("[TopK][UpdAdd]Received %d:%d. Minimum: %v\n", addOp.Id, addOp.Score, crdt.smallestScore)
	elem, hasId := crdt.elems[addOp.Id]
	if hasId && elem.Score >= addOp.Score {
		//fmt.Println("[TopK][UpdAdd]Returning noop as ID is already known with a higher score.")
		return NoOp{}
	}
	if hasId || len(crdt.elems) < crdt.maxElems || addOp.isHigherScore(crdt.smallestScore) ||
		crdt.smallestScore.Score == addOp.Score && crdt.keepTiedEntries {
		//fmt.Println("[TopK][UpdAdd]Generating downstream")
		data := addOp.Data
		if data == nil {
			data = &[]byte{}
		}
		return DownstreamSimpleTopKAdd{TopKScore: TopKScore{Id: addOp.Id, Score: addOp.Score, Data: addOp.Data}, ToReplicate: new(bool)}
	}
	//fmt.Println("[TopK][UpdAdd]Returning noOp due to top beng full.")
	return NoOp{}
}

func (crdt *TopKCrdt) isTiedWithMin(score TopKScore) bool {
	return crdt.keepTiedEntries && score.Score == crdt.smallestScore.Score && score.Id != crdt.smallestScore.Id
}

// TODO: Consider improving this to avoid unecessary elements going into downstream
// (As of now, all elements above the curent minimum go to downstream, without taking into
// consideration the elements already processed.)
func (crdt *TopKCrdt) getTopKAddAllDownstreamArgs(addOp *TopKAddAll) (args DownstreamArguments) {
	emptyData := &[]byte{}
	if len(crdt.elems) == 0 { //Initialization. So no need to check with existing elements or smallestScore.
		if len(addOp.Scores) < 2*crdt.maxElems { //Replicate everything, even though some will not enter the TopK for sure.
			for i, score := range addOp.Scores {
				if score.Data == nil {
					addOp.Scores[i].Data = emptyData
				}
			}
			return DownstreamSimpleTopKAddAll{DownstreamAdds: addOp.Scores}
		} else {
			if crdt.maxElems <= 100 && len(addOp.Scores) >= crdt.maxElems*5 { //Use a maxBuffer.
				maxBuf := newMaxBuffer[TopKScore](crdt.maxElems, MIN_SCORE)
				for _, score := range addOp.Scores {
					if score.Data == nil {
						score.Data = emptyData
					}
					maxBuf.addIfInBetween(score, maxBuf.Len()) //Passing maxBuf.Len() ensures that if the buffer isn't full, even new "mins" will be added to the buffer.
				}
				args = DownstreamSimpleTopKAddAll{DownstreamAdds: maxBuf.maxs}
			} else { //Better copy everything, sort and then filter. Note that we already know that len(addOp.Scores) is, at least, 2*maxElems
				downAdds := make([]TopKScore, len(addOp.Scores))
				copy(downAdds, addOp.Scores)
				sort.Slice(downAdds, func(i, j int) bool { return downAdds[i].isHigherScore(downAdds[j]) })
				for i, score := range downAdds {
					if score.Data == nil {
						downAdds[i].Data = emptyData
					}
				}
				args = DownstreamSimpleTopKAddAll{DownstreamAdds: downAdds[:crdt.maxElems]}
			}
		}
	} else {
		downAdds := make([]TopKScore, len(addOp.Scores))
		nAdd := 0
		hasId, existingElem := false, TopKScore{}
		for _, add := range addOp.Scores {
			existingElem, hasId = crdt.elems[add.Id]
			if hasId && existingElem.Score >= add.Score {
				continue
			}
			if hasId || len(crdt.elems) < crdt.maxElems || add.isHigherScore(crdt.smallestScore) ||
				(crdt.smallestScore.Score == add.Score && crdt.keepTiedEntries) {
				data := add.Data
				if data == nil {
					data = emptyData
				}
				downAdds[nAdd] = TopKScore{Id: add.Id, Score: add.Score, Data: add.Data}
				nAdd++
			}
		}
		if nAdd == 0 {
			return NoOp{}
		}
		if nAdd == 1 {
			return DownstreamSimpleTopKAdd{TopKScore: downAdds[0], ToReplicate: new(bool)}
		}
		args = DownstreamSimpleTopKAddAll{DownstreamAdds: downAdds[:nAdd]}
	}

	//fmt.Printf("[TopK][GetTopKAddAllDownArgs]DownAdds: %+v\n", downAdds[:nAdd])
	return args
}

func (crdt *TopKCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		//fmt.Printf("[TOPK]Original args type: %T.\n", downstreamArgs)
		for _, upd := range multiUpd {
			crdt.Downstream(updTs, upd.(DownstreamArguments))
		}
		return nil
	}
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)
	return
}

func (crdt *TopKCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect *Effect) {
	//fmt.Printf("[TopK]Apply downstream. Operation: %+v (Type: %T)\n", downstreamArgs, downstreamArgs)
	switch opType := downstreamArgs.(type) {
	case DownstreamSimpleTopKAdd:
		effect = crdt.applyAdd(opType)
	case DownstreamSimpleTopKAddAll:
		effect = crdt.applyAddAll(opType)
	case TopKInit:
		effect = crdt.applyInit(opType)
	default:
		fmt.Printf("[TopK][Downstream]Unsupported downstream type %v (%T)\n", downstreamArgs, downstreamArgs)
	}
	return
}

func (crdt *TopKCrdt) applyInit(op TopKInit) (effect *Effect) {
	if int(op) > crdt.maxElems*10 && len(crdt.elems) == 0 {
		crdt.elems = make(map[int32]TopKScore, int(op)) //Resize.
	}
	crdt.maxElems = int(op)
	var effectValue Effect = NoEffect{}
	effect = &effectValue
	//fmt.Println("[TOPK]Max top size set to", crdt.maxElems)
	return
}

func (crdt *TopKCrdt) applyAdd(op DownstreamSimpleTopKAdd) (effect *Effect) {
	//fmt.Printf("[TopK][DownstreamAdd]Received: %d:%d (Min: %d:%d)\n", op.Id, op.Score, crdt.smallestScore.Id, crdt.smallestScore.Score)
	elem, has := crdt.elems[op.Id]
	var effectI Effect = NoEffect{}
	topChanged := false
	if has && elem.Score > op.Score { //Old score is higher. Ignore.
		//fmt.Printf("[TopK][DownstreamAdd]Already have ID but ignored as new value is lower: %d:%d (old: %d %d) (Min: %d:%d)\n", op.Id, op.Score,
		//elem.Id, elem.Score, crdt.smallestScore.Id, crdt.smallestScore.Score)
		*op.ToReplicate = false
	} else if has { //New score is higher, goes to top.
		*op.ToReplicate, topChanged = true, true
		effectI = TopKReplaceEffect{newElem: op.TopKScore, oldElem: crdt.elems[op.Id], oldMin: crdt.smallestScore}
		crdt.elems[op.Id] = op.TopKScore
		if crdt.smallestScore.Id == op.Id { //The id updated used to be the smallest score
			crdt.findAndUpdateMin()
		}
		//fmt.Printf("[TopK][DownstreamAdd]Already have ID but ignored as new value is lower: %d:%d (old: %d %d) (Min: %d:%d)\n", op.Id, op.Score,
		//elem.Id, elem.Score, crdt.smallestScore.Id, crdt.smallestScore.Score)
	} else if len(crdt.elems) < crdt.maxElems { //Space in the top, new elem, so it goes in.
		*op.ToReplicate, topChanged = true, true
		crdt.elems[op.Id] = op.TopKScore
		if op.TopKScore.isLowerScore(crdt.smallestScore) {
			effectI = TopKReplaceEffect{newElem: op.TopKScore, oldMin: crdt.smallestScore}
			//fmt.Printf("[TopK][DownstreamAdd]Top not full yet. Min changed. OldMin: %d:%d. NewMin: %d:%d\n",
			//crdt.smallestScore.Id, crdt.smallestScore.Score, op.TopKScore.Id, op.TopKScore.Score)
			crdt.smallestScore = op.TopKScore
		} else {
			//fmt.Printf("[TopK][DownstreamAdd]Top not full yet. Min not changed. Min: %d:%d\n",
			//crdt.smallestScore.Id, crdt.smallestScore.Score)
			effectI = TopKAddEffect{TopKScore: op.TopKScore}
		}
	} else if op.TopKScore.isHigherScore(crdt.smallestScore) { //!has and the topK is full
		*op.ToReplicate, topChanged = true, true
		delete(crdt.elems, crdt.smallestScore.Id)
		crdt.elems[op.Id] = op.TopKScore
		if op.TopKScore.Score == crdt.smallestScore.Score { //Same score, but the new one has a higher ID
			if crdt.keepTiedEntries {
				crdt.addTiedElem(crdt.smallestScore)
				effectI = TopKReplaceAndTiedEffect{newElem: op.TopKScore, oldElem: crdt.smallestScore, oldMin: crdt.smallestScore}
			}
			//fmt.Printf("[TopK][DownstreamAdd]New add has equal value to min, but higher Id. TopK is full. OldMin: %d:%d. NewMin: %d:%d\n",
			//crdt.smallestScore.Id, crdt.smallestScore.Score, op.TopKScore.Id, op.TopKScore.Score)
			crdt.smallestScore = op.TopKScore
		} else {
			oldMin := crdt.smallestScore
			crdt.findAndUpdateMin()
			if crdt.keepTiedEntries && oldMin.Score == crdt.smallestScore.Score {
				crdt.addTiedElem(oldMin)
				effectI = TopKReplaceAndTiedEffect{newElem: op.TopKScore, oldElem: crdt.smallestScore, oldMin: crdt.smallestScore}
			} else if crdt.keepTiedEntries {
				effectI = TopKReplaceAndCleanTiedEffect{newElem: op.TopKScore, oldElem: crdt.smallestScore, oldMin: crdt.smallestScore, oldTied: crdt.tiedElems}
				crdt.tiedElems = nil //New min has a higher score
			}
			//fmt.Printf("[TopK][DownstreamAdd]New add is higher than min. TopK is full. OldMin: %d:%d. NewMin: %d:%d\n",
			//oldMin.Id, oldMin.Score, crdt.smallestScore.Id, crdt.smallestScore.Score)
		}
		if (effectI == NoEffect{}) {
			effectI = TopKReplaceEffect{newElem: op.TopKScore, oldElem: crdt.smallestScore, oldMin: crdt.smallestScore}
		}
	} else { //Topk is full and the elem is too small. However, if it ties with smallestScore and we keep tied entries, we keep it as tied.
		if crdt.smallestScore.Score == op.TopKScore.Score && crdt.keepTiedEntries {
			crdt.addTiedElem(op.TopKScore)
			*op.ToReplicate, effectI, topChanged = true, TopKAddEffect{TopKScore: op.TopKScore}, true
		} else {
			*op.ToReplicate, effectI = false, NoEffect{}
		}
		//fmt.Printf("[TopK][DownstreamAdd]New add is lower than min. TopK is full. Nothing changed.")
	}
	if topChanged {
		crdt.nUpds++
	}
	return &effectI
}

func (crdt *TopKCrdt) addTiedElem(score TopKScore) {
	if crdt.tiedElems == nil {
		crdt.tiedElems = make(map[int32]TopKScore)
	}
	crdt.tiedElems[score.Id] = score
}

func (crdt *TopKCrdt) applyAddAll(op DownstreamSimpleTopKAddAll) (effect *Effect) {
	currI, topChanged := 0, false
	elem, has := TopKScore{}, false
	listEffect := TopKAddAllEffect{effects: make([]Effect, len(op.DownstreamAdds))}
	newDown := make([]TopKScore, len(op.DownstreamAdds))
	var effectI Effect
	for _, add := range op.DownstreamAdds {
		elem, has = crdt.elems[add.Id]
		if has && elem.Score > add.Score { //Ignore, as the existing score is higher.
			/*if currI < len(op.DownstreamAdds)-1 {
				op.DownstreamAdds[currI] = elem
			}*/
		} else if has { //New score is higher, goes to top.
			listEffect.effects[currI] = TopKReplaceEffect{newElem: add, oldElem: crdt.elems[add.Id], oldMin: crdt.smallestScore}
			crdt.elems[add.Id], newDown[currI] = add, add
			if crdt.smallestScore.Id == add.Id { //The id updated used to be the smallest score
				crdt.findAndUpdateMin()
			}
			currI++
			topChanged = true
		} else if len(crdt.elems) < crdt.maxElems { //Space in the top, new elem, so it goes in.
			crdt.elems[add.Id], newDown[currI] = add, add
			if add.isLowerScore(crdt.smallestScore) {
				listEffect.effects[currI] = TopKReplaceEffect{newElem: add, oldMin: crdt.smallestScore}
				crdt.smallestScore = add
			} else {
				listEffect.effects[currI] = TopKAddEffect{TopKScore: add}
			}
			currI++
			topChanged = true
		} else if add.isHigherScore(crdt.smallestScore) { //TopK is full, but it is higher than smallest score. So it will go in.
			topChanged = true
			crdt.elems[add.Id], newDown[currI] = add, add
			delete(crdt.elems, crdt.smallestScore.Id)
			if add.Score == crdt.smallestScore.Score { //Same score, but the new one has a higher ID
				if crdt.keepTiedEntries {
					crdt.addTiedElem(crdt.smallestScore)
					listEffect.effects[currI] = TopKReplaceAndTiedEffect{newElem: add, oldElem: crdt.smallestScore, oldMin: crdt.smallestScore}
				}
				crdt.smallestScore = add
			} else {
				if crdt.keepTiedEntries {
					listEffect.effects[currI] = TopKReplaceAndCleanTiedEffect{newElem: add, oldElem: crdt.smallestScore, oldMin: crdt.smallestScore, oldTied: crdt.tiedElems}
					crdt.tiedElems = nil
				}
				crdt.findAndUpdateMin()
			}
			currI++
		} else { //Topk is full and the elem is too small. However, if it ties with smallestScore and we keep tied entries, we keep it as tied.
			if crdt.smallestScore.Score == add.Score && crdt.keepTiedEntries {
				crdt.addTiedElem(add)
				listEffect.effects[currI], newDown[currI] = TopKAddEffect{TopKScore: add}, add
				currI++
				topChanged = true
			} else if currI < len(op.DownstreamAdds)-1 { //Ignore the current entry
				//op.DownstreamAdds[currI] = elem
			}
		}
	}
	op.DownstreamAdds, listEffect.effects = newDown[:currI], listEffect.effects[:currI]
	if topChanged {
		crdt.nUpds++
	}
	if currI == 0 {
		effectI = NoEffect{}
		return &effectI
	}
	effectI = listEffect
	return &effectI
}

func (crdt *TopKCrdt) findAndUpdateMin() {
	minSoFar := TopKScore{}
	for _, elem := range crdt.elems {
		if elem.isLowerScore(minSoFar) {
			minSoFar = elem
		}
	}
	crdt.smallestScore = minSoFar
}

func (crdt *TopKCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

//METHODS FOR INVERSIBLE CRDT

func (crdt *TopKCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := TopKCrdt{
		CRDTVM:        crdt.CRDTVM.copy(),
		replicaID:     crdt.replicaID,
		maxElems:      crdt.maxElems,
		smallestScore: crdt.smallestScore,
		elems:         make(map[int32]TopKScore),
	}

	for id, elem := range crdt.elems {
		newCrdt.elems[id] = elem
	}

	return &newCrdt
}

func (crdt *TopKCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *TopKCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *TopKCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case TopKAddEffect:
		crdt.undoAddEffect(&typedEffect)
	case TopKReplaceEffect:
		crdt.undoReplaceEffect(&typedEffect)
	case TopKReplaceAndTiedEffect:
		crdt.undoReplaceAndTiedEffect(&typedEffect)
	case TopKReplaceAndCleanTiedEffect:
		crdt.undoReplaceAndCleanTiedEffect(&typedEffect)
	case TopKAddAllEffect:
		crdt.undoAddAllEffect(&typedEffect)
	}
}

func (crdt *TopKCrdt) undoAddEffect(effect *TopKAddEffect) {
	delete(crdt.elems, effect.Id)
	if crdt.smallestScore.Score == effect.TopKScore.Score && crdt.keepTiedEntries {
		delete(crdt.tiedElems, effect.Id)
	}
}

func (crdt *TopKCrdt) undoReplaceEffect(effect *TopKReplaceEffect) {
	delete(crdt.elems, effect.newElem.Id)
	if (effect.oldElem != TopKScore{}) {
		crdt.elems[effect.oldElem.Id] = effect.oldElem
	}
	crdt.smallestScore = effect.oldMin

}

func (crdt *TopKCrdt) undoReplaceAndTiedEffect(effect *TopKReplaceAndTiedEffect) {
	delete(crdt.elems, effect.newElem.Id)
	crdt.elems[effect.oldElem.Id] = effect.oldElem
	delete(crdt.tiedElems, effect.oldMin.Id)
	crdt.smallestScore = effect.oldMin
}

func (crdt *TopKCrdt) undoReplaceAndCleanTiedEffect(effect *TopKReplaceAndCleanTiedEffect) {
	crdt.tiedElems = effect.oldTied
	delete(crdt.elems, effect.newElem.Id)
	if (effect.oldElem != TopKScore{}) {
		crdt.elems[effect.oldElem.Id] = effect.oldElem
	}
	crdt.smallestScore = effect.oldMin
}

func (crdt *TopKCrdt) undoAddAllEffect(effect *TopKAddAllEffect) {
	for _, eff := range effect.effects {
		crdt.undoEffect(&eff)
	}
}

func (crdt *TopKCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

// Protobuf functions
func (crdtOp TopKInit) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	init := protobuf.GetTopkinitop()
	return TopKInit(init.GetTopSize())
}

func (crdtOp TopKInit) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Topkinitop{Topkinitop: &proto.ApbTopKInit{TopSize: pb.Uint32(uint32(crdtOp)), TopType: proto.CRDTType_TOPK.Enum()}}}
}

func (downOp TopKInit) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return TopKInit(protobuf.GetTopkinitOp().GetTopSize())
}

func (downOp TopKInit) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkinitOp{TopkinitOp: &proto.ProtoTopKInitDownstream{TopSize: pb.Uint32(uint32(downOp)), TopType: proto.CRDTType_TOPK.Enum()}}}
}

func (downOp DownstreamSimpleTopKAdd) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	addProto, toReplicate := protobuf.GetTopkOp().GetAdds()[0], true
	downOp.Id, downOp.Score, downOp.Data, downOp.ToReplicate = addProto.GetId(), addProto.GetScore(), tools.ByteSliceGetOrDefault(addProto.Data, emptyData), &toReplicate
	return downOp
}

func (downOp DownstreamSimpleTopKAdd) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	add := &proto.ProtoTopKScore{Id: pb.Int32(downOp.Id), Score: pb.Int32(downOp.Score)}
	if downOp.Data != nil && len(*downOp.Data) > 0 {
		add.Data = *downOp.Data
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkOp{TopkOp: &proto.ProtoTopKDownstream{Adds: []*proto.ProtoTopKScore{add}}}}
}

func (downOp DownstreamSimpleTopKAddAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	addProto := protobuf.GetTopkOp().GetAdds()
	downOp.DownstreamAdds = make([]TopKScore, len(addProto))
	for i, addP := range addProto {
		downOp.DownstreamAdds[i] = TopKScore{Id: addP.GetId(), Score: addP.GetScore(), Data: &addP.Data}
	}
	return downOp
}

func (downOp DownstreamSimpleTopKAddAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoAdds := make([]*proto.ProtoTopKScore, len(downOp.DownstreamAdds))
	var curr proto.ProtoTopKScore
	for i, add := range downOp.DownstreamAdds {
		curr = proto.ProtoTopKScore{Id: pb.Int32(add.Id), Score: pb.Int32(add.Score)}
		if add.Data != nil && len(*add.Data) > 0 {
			curr.Data = *add.Data
		}
		protoAdds[i] = &curr
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_TopkOp{TopkOp: &proto.ProtoTopKDownstream{Adds: protoAdds}}}
}

func (crdt *TopKCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	protoElems := make([]*proto.ProtoTopKScore, len(crdt.elems))
	i, j := 0, 0
	for _, elem := range crdt.elems {
		protoElems[i] = &proto.ProtoTopKScore{Id: &elem.Id, Score: &elem.Score, Data: *elem.Data}
		i++
	}
	topKState := proto.ProtoTopKState{Elems: protoElems, MaxElems: pb.Int32(int32(crdt.maxElems)), KeepTiedEntries: pb.Bool(crdt.keepTiedEntries)}
	if crdt.keepTiedEntries && len(crdt.tiedElems) > 0 {
		tiedProto := make([]*proto.ProtoTopKScore, len(crdt.tiedElems))
		for _, tied := range crdt.tiedElems {
			tiedProto[j] = &proto.ProtoTopKScore{Id: &tied.Id, Score: &tied.Score, Data: *tied.Data}
		}
		topKState.Tied = tiedProto
	}
	smallest := &proto.ProtoTopKScore{Id: &crdt.smallestScore.Id, Score: &crdt.smallestScore.Score, Data: *crdt.smallestScore.Data}
	topKState.Smallest = smallest
	return &proto.ProtoState{State: &proto.ProtoState_Topk{Topk: &topKState}}
}

func (crdt *TopKCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID uint16) (newCDRT CRDT) {
	topKProto := proto.GetTopk()
	elems := make(map[int32]TopKScore)
	keepTiedEntries := topKProto.GetKeepTiedEntries()

	for _, protoScore := range topKProto.GetElems() {
		data := protoScore.GetData()
		elems[protoScore.GetId()] = TopKScore{Id: protoScore.GetId(), Score: protoScore.GetScore(), Data: &data}
	}
	smallestProto := topKProto.GetSmallest()
	smallestData := smallestProto.GetData()
	smallestScore := TopKScore{Id: smallestProto.GetId(), Score: smallestProto.GetScore(), Data: &smallestData}
	var tiedMap map[int32]TopKScore
	if keepTiedEntries {
		tiedElems := topKProto.GetTied()
		if len(tiedElems) > 0 {
			tiedMap = make(map[int32]TopKScore)
			for _, scoreP := range tiedElems {
				data := scoreP.GetData()
				tiedMap[scoreP.GetId()] = TopKScore{Id: scoreP.GetId(), Score: scoreP.GetScore(), Data: &data}
			}
		}
	}
	return (&TopKCrdt{elems: elems, smallestScore: smallestScore, maxElems: int(topKProto.GetMaxElems()),
		keepTiedEntries: keepTiedEntries, tiedElems: tiedMap}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *TopKCrdt) GetCRDT() CRDT { return crdt }
