package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"sort"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

//A TopK without support for removes.
//Check TopKRmv for a top with support for removes and TopSum for a top with support for incs and decs.
//Use this TopK whenever removes are not required, as not supporting removes
//Allows this TopK to store less entries and has less complex update processing.

type TopKCrdt struct {
	CRDTVM
	vc        clocksi.Timestamp
	replicaID int16

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

func (crdt *TopKCrdt) GetCRDTType() proto.CRDTType                  { return proto.CRDTType_TOPK }
func (args DownstreamSimpleTopKAdd) GetCRDTType() proto.CRDTType    { return proto.CRDTType_TOPK }
func (args DownstreamSimpleTopKAddAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_TOPK }
func (args DownstreamSimpleTopKAdd) MustReplicate() bool            { return true }
func (args DownstreamSimpleTopKAddAll) MustReplicate() bool         { return true }

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

func (crdt *TopKCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return crdt.InitializeWithSize(startTs, replicaID, defaultTopKSize)
}

func (crdt *TopKCrdt) InitializeWithSize(startTs *clocksi.Timestamp, replicaID int16, size int) (newCrdt CRDT) {
	crdt = &TopKCrdt{
		CRDTVM:          (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		vc:              clocksi.NewClockSiTimestamp(),
		replicaID:       replicaID,
		maxElems:        100,
		smallestScore:   TopKScore{},
		elems:           make(map[int32]TopKScore),
		keepTiedEntries: true,
	}
	newCrdt = crdt
	return
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *TopKCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *TopKCrdt) {
	crdt.CRDTVM, crdt.replicaID = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete), replicaID
	return crdt
}

func (crdt *TopKCrdt) IsBigCRDT() bool { return crdt.maxElems > 100 && len(crdt.elems) > 100 }

func (crdt *TopKCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	//TODO: Consider updsNotYetApplied in all of these
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
}

func (crdt *TopKCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	if !crdt.keepTiedEntries {
		values := make([]TopKScore, len(crdt.elems))
		i := 0
		for _, elem := range crdt.elems {
			values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
			i++
		}
		return TopKValueState{Scores: values}
	}
	values := make([]TopKScore, len(crdt.elems)+len(crdt.tiedElems))
	i := 0
	for _, elem := range crdt.elems {
		values[i] = TopKScore{Id: elem.Id, Score: elem.Score, Data: elem.Data}
		i++
	}
	for _, tied := range crdt.tiedElems {
		values[i] = TopKScore{Id: tied.Id, Score: tied.Score, Data: tied.Data}
		i++
	}
	return TopKValueState{Scores: values}
}

/*
Note: in the current implementation, at most N entries are returned, even if N+1 has the same value as N.
*/
func (crdt *TopKCrdt) getTopN(numberEntries int32, updsNotYetApplied []UpdateArguments) (state State) {
	if crdt.sortedElems == nil {
		//TODO: May be an issue when updsNotYetApplied get considered.
		crdt.sortedElems = crdt.getState(updsNotYetApplied).(TopKValueState).Scores
		sort.Slice(crdt.sortedElems, func(i, j int) bool { return crdt.sortedElems[i].Score > crdt.sortedElems[j].Score })
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
		if crdt.keepTiedEntries && crdt.smallestScore.Score >= minValue {
			for _, tied := range crdt.tiedElems {
				values = append(values, tied)
			}
		}
	}

	return TopKValueState{Scores: values[:actuallyAdded]}
}

func (crdt *TopKCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch opType := args.(type) {
	case TopKAdd:
		downstreamArgs = crdt.getTopKAddDownstreamArgs(&opType)
	case TopKAddAll:
		downstreamArgs = crdt.getTopKAddAllDownstreamArgs(&opType)
	case TopKInit:
		downstreamArgs = opType
	}
	return
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
				data = &[]byte{}
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
	//fmt.Printf("[TopK][GetTopKAddAllDownArgs]DownAdds: %+v\n", downAdds[:nAdd])
	return DownstreamSimpleTopKAddAll{DownstreamAdds: downAdds[:nAdd]}
}

func (crdt *TopKCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)
	return
}

func (crdt *TopKCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect *Effect) {
	//fmt.Printf("[TopK]Apply downstream. Operation: %+v (Type: %T)\n", downstreamArgs, downstreamArgs)
	switch opType := downstreamArgs.(type) {
	case DownstreamSimpleTopKAdd:
		effect = crdt.applyAdd(&opType)
	case DownstreamSimpleTopKAddAll:
		effect = crdt.applyAddAll(&opType)
	case TopKInit:
		effect = crdt.applyInit(&opType)
	}
	return
}

func (crdt *TopKCrdt) applyInit(op *TopKInit) (effect *Effect) {
	crdt.maxElems = int(op.TopSize)
	var effectValue Effect = NoEffect{}
	effect = &effectValue
	//fmt.Println("[TOPK]Max top size set to", crdt.maxElems)
	return
}

func (crdt *TopKCrdt) applyAdd(op *DownstreamSimpleTopKAdd) (effect *Effect) {
	//fmt.Printf("[TopK][DownstreamAdd]Received: %d:%d (Min: %d:%d)\n", op.Id, op.Score, crdt.smallestScore.Id, crdt.smallestScore.Score)
	elem, has := crdt.elems[op.Id]
	var effectI Effect = NoEffect{}
	if has && elem.Score > op.Score {
		//fmt.Printf("[TopK][DownstreamAdd]Already have ID but ignored as new value is lower: %d:%d (old: %d %d) (Min: %d:%d)\n", op.Id, op.Score,
		//elem.Id, elem.Score, crdt.smallestScore.Id, crdt.smallestScore.Score)
		*op.ToReplicate = false
	} else if has {
		*op.ToReplicate = true
		effectI = TopKReplaceEffect{newElem: op.TopKScore, oldElem: crdt.elems[op.Id], oldMin: crdt.smallestScore}
		crdt.elems[op.Id] = op.TopKScore
		if crdt.smallestScore.Id == op.Id { //The id updated used to be the smallest score
			crdt.findAndUpdateMin()
		}
		//fmt.Printf("[TopK][DownstreamAdd]Already have ID but ignored as new value is lower: %d:%d (old: %d %d) (Min: %d:%d)\n", op.Id, op.Score,
		//elem.Id, elem.Score, crdt.smallestScore.Id, crdt.smallestScore.Score)
	} else if len(crdt.elems) < crdt.maxElems {
		*op.ToReplicate = true
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
		*op.ToReplicate = true
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
	} else {
		if crdt.smallestScore.Score == op.TopKScore.Score && crdt.keepTiedEntries {
			crdt.addTiedElem(op.TopKScore)
			*op.ToReplicate, effectI = true, TopKAddEffect{TopKScore: op.TopKScore}
		} else {
			*op.ToReplicate, effectI = false, NoEffect{}
		}
		//fmt.Printf("[TopK][DownstreamAdd]New add is lower than min. TopK is full. Nothing changed.")
	}
	return &effectI
}

func (crdt *TopKCrdt) addTiedElem(score TopKScore) {
	if crdt.tiedElems == nil {
		crdt.tiedElems = make(map[int32]TopKScore)
	}
	crdt.tiedElems[score.Id] = score
}

func (crdt *TopKCrdt) applyAddAll(op *DownstreamSimpleTopKAddAll) (effect *Effect) {
	currI := 0
	elem, has := TopKScore{}, false
	listEffect := TopKAddAllEffect{effects: make([]Effect, len(op.DownstreamAdds))}
	newDown := make([]TopKScore, len(op.DownstreamAdds))
	var effectI Effect
	for _, add := range op.DownstreamAdds {
		elem, has = crdt.elems[add.Id]
		if has && elem.Score > add.Score {
			/*if currI < len(op.DownstreamAdds)-1 {
				op.DownstreamAdds[currI] = elem
			}*/
		} else if has {
			listEffect.effects[currI] = TopKReplaceEffect{newElem: add, oldElem: crdt.elems[add.Id], oldMin: crdt.smallestScore}
			crdt.elems[add.Id], newDown[currI] = add, add
			if crdt.smallestScore.Id == add.Id { //The id updated used to be the smallest score
				crdt.findAndUpdateMin()
			}
			currI++
		} else if len(crdt.elems) < crdt.maxElems {
			crdt.elems[add.Id], newDown[currI] = add, add
			if add.isLowerScore(crdt.smallestScore) {
				listEffect.effects[currI] = TopKReplaceEffect{newElem: add, oldMin: crdt.smallestScore}
				crdt.smallestScore = add
			} else {
				listEffect.effects[currI] = TopKAddEffect{TopKScore: add}
			}
			currI++
		} else if add.isHigherScore(crdt.smallestScore) {
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
		} else {
			if crdt.smallestScore.Score == add.Score && crdt.keepTiedEntries {
				crdt.addTiedElem(add)
				listEffect.effects[currI], newDown[currI] = TopKAddEffect{TopKScore: add}, add
				currI++
			} else if currI < len(op.DownstreamAdds)-1 { //Ignore the current entry
				//op.DownstreamAdds[currI] = elem
			}
		}
	}
	op.DownstreamAdds, listEffect.effects = newDown[:currI], listEffect.effects[:currI]
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
func (downOp DownstreamSimpleTopKAdd) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	addProto, toReplicate := protobuf.GetTopkOp().GetAdds()[0], true
	downOp.Id, downOp.Score, downOp.Data, downOp.ToReplicate = addProto.GetId(), addProto.GetScore(), &addProto.Data, &toReplicate
	return downOp
}

func (downOp DownstreamSimpleTopKAdd) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{TopkOp: &proto.ProtoTopKDownstream{Adds: []*proto.ProtoTopKScore{{
		Id: pb.Int32(downOp.Id), Score: pb.Int32(downOp.Score), Data: *downOp.Data,
	}}}}
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
	for i, add := range downOp.DownstreamAdds {
		protoAdds[i] = &proto.ProtoTopKScore{Id: pb.Int32(add.Id), Score: pb.Int32(add.Score), Data: *add.Data}
	}
	return &proto.ProtoOpDownstream{TopkOp: &proto.ProtoTopKDownstream{Adds: protoAdds}}
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
	return &proto.ProtoState{Topk: &topKState}
}

func (crdt *TopKCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCDRT CRDT) {
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
