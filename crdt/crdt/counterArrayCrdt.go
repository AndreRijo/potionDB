package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	pb "google.golang.org/protobuf/proto"
)

// IMPORTANT NOTE: CounterArraySetSize must be used before any increment or decrement operations,
// or when it is known that no concurrent operations will be happening.
// While CounterArraySetSize commutes with CounterArrayIncrement/Decrement and
// CounterArrayIncrement/DecrementSub, it does not with CounterArrayIncrement/DecrementAll
// If one must be able to use CounterArraySetSize concurrently with inc/dec, in order to ensure correctness
// do not use CounterArrayIncrement/DecrementAll.
// CounterArraySetSize does commute with CounterArrayIncrement/DecrementMulti, as follows.
// It will only increment/decrement as many positions as the values in the operation, and grows the array
// as needed. If the array size is >= len(Changes), due to a concurrent CounterArraySetSize, the extra positions
// will remain unchanged.
type CounterArrayCrdt struct {
	CRDTVM
	counts []int64
}

//States

type CounterArrayState []int64

type CounterArraySingleState int64

//Reads

// Position
type CounterArraySingleArguments int32
type CounterArrayExceptArguments int32

// Positions
type CounterArraySubArguments []int32

type CounterArrayExceptRangeArguments struct {
	ExceptRange    []int32 //Ranges to skip. Evens: start. Odds: end.
	NPositionsSkip int32   //Optional. This helps to create a read slice with a more appropriate dimension.
}

//Updates

type CounterArraySetSize int32

type CounterArrayIncrement struct {
	Change   int64
	Position int32
}

type CounterArrayDecrement struct {
	Change   int64
	Position int32
}

// If len(Changes) == 1, then increments all positions in Positions by Changes[0]
// Otherwise, assumes len(changes) == len(positions)
type CounterArrayIncrementSub struct {
	Changes   []int64
	Positions []int32
}

type CounterArrayDecrementSub struct {
	Changes   []int64
	Positions []int32
}

type CounterArrayIncrementAll int64
type CounterArrayDecrementAll int64

// []int64: Changes.
type CounterArrayIncrementMulti []int64
type CounterArrayDecrementMulti []int64

type CounterArrayIncrementEffect CounterArrayIncrement
type CounterArrayDecrementEffect CounterArrayDecrement
type CounterArrayIncrementSubEffect CounterArrayIncrementSub
type CounterArrayDecrementSubEffect CounterArrayDecrementSub
type CounterArrayIncrementAllEffect CounterArrayIncrementAll
type CounterArrayDecrementAllEffect CounterArrayDecrementAll
type CounterArrayIncrementMultiEffect CounterArrayIncrementMulti
type CounterArrayDecrementMultiEffect CounterArrayDecrementMulti
type CounterArraySetSizeEffect CounterArraySetSize

//Not needed as, in this case, the value will be lost when shrinking the array.
/*type CounterArrayIncWithSizeEffect struct {
	Change  int64
	Position int32
	OldSize int
}
type CounterArrayDecWithSizeEffect struct {
	Change  int64
	Position int32
	OldSize int
}*/
type CounterArrayIncSubWithSizeEffect struct {
	IncEff  CounterArrayIncrementSubEffect
	OldSize int
}
type CounterArrayDecSubWithSizeEffect struct {
	DecEff  CounterArrayDecrementSubEffect
	OldSize int
}
type CounterArrayIncMultiWithSizeEffect struct {
	IncEff  CounterArrayIncrementMultiEffect
	OldSize int
}
type CounterArrayDecMultiWithSizeEffect struct {
	DecEff  CounterArrayDecrementMultiEffect
	OldSize int
}

func (crdt *CounterArrayCrdt) GetCRDTType() proto.CRDTType     { return proto.CRDTType_ARRAY_COUNTER }
func (args CounterArraySetSize) GetCRDTType() proto.CRDTType   { return proto.CRDTType_ARRAY_COUNTER }
func (args CounterArrayIncrement) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_COUNTER }
func (args CounterArrayDecrement) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_COUNTER }
func (args CounterArrayIncrementSub) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArrayDecrementSub) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArrayIncrementAll) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArrayDecrementAll) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArrayIncrementMulti) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArrayDecrementMulti) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (state CounterArrayState) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_COUNTER }
func (state CounterArrayState) GetREADType() proto.READType { return proto.READType_FULL }
func (state CounterArraySingleState) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (state CounterArraySingleState) GetREADType() proto.READType {
	return proto.READType_COUNTER_SINGLE
}
func (args CounterArraySetSize) MustReplicate() bool        { return true }
func (args CounterArrayIncrement) MustReplicate() bool      { return true }
func (args CounterArrayDecrement) MustReplicate() bool      { return true }
func (args CounterArrayIncrementSub) MustReplicate() bool   { return true }
func (args CounterArrayDecrementSub) MustReplicate() bool   { return true }
func (args CounterArrayIncrementAll) MustReplicate() bool   { return true }
func (args CounterArrayDecrementAll) MustReplicate() bool   { return true }
func (args CounterArrayIncrementMulti) MustReplicate() bool { return true }
func (args CounterArrayDecrementMulti) MustReplicate() bool { return true }
func (args CounterArraySingleArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArraySubArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArrayExceptArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArrayExceptRangeArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COUNTER
}
func (args CounterArraySingleArguments) GetREADType() proto.READType {
	return proto.READType_COUNTER_SINGLE
}
func (args CounterArraySubArguments) GetREADType() proto.READType { return proto.READType_COUNTER_SUB }
func (args CounterArrayExceptArguments) GetREADType() proto.READType {
	return proto.READType_COUNTER_EXCEPT
}
func (args CounterArrayExceptRangeArguments) GetREADType() proto.READType {
	return proto.READType_COUNTER_EXCEPT_RANGE
}
func (args CounterArraySingleArguments) HasInnerReads() bool      { return false }
func (args CounterArraySubArguments) HasInnerReads() bool         { return false }
func (args CounterArrayExceptArguments) HasInnerReads() bool      { return false }
func (args CounterArrayExceptRangeArguments) HasInnerReads() bool { return false }
func (args CounterArraySingleArguments) HasVariables() bool       { return false }
func (args CounterArraySubArguments) HasVariables() bool          { return false }
func (args CounterArrayExceptArguments) HasVariables() bool       { return false }
func (args CounterArrayExceptRangeArguments) HasVariables() bool  { return false }

func (crdt *CounterArrayCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &CounterArrayCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		counts: make([]int64, 1),
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *CounterArrayCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *CounterArrayCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *CounterArrayCrdt) IsBigCRDT() bool { return len(crdt.counts) >= 1000 }

func (crdt *CounterArrayCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case CounterArraySingleArguments:
		return crdt.getSingleState(updsNotYetApplied, int32(typedArgs))
	case CounterArrayExceptArguments:
		if int(typedArgs) >= len(crdt.counts) {
			return crdt.getState(updsNotYetApplied)
		}
		return crdt.getExceptState(updsNotYetApplied, int32(typedArgs))
	case CounterArraySubArguments:
		return crdt.getSubState(updsNotYetApplied, []int32(typedArgs))
	case CounterArrayExceptRangeArguments:
		return crdt.getExceptRangeState(updsNotYetApplied, typedArgs.ExceptRange, typedArgs.NPositionsSkip)
	default:
		fmt.Printf("[CounterArrayCrdt] Unknown read type: %+v\n", args)
	}
	return nil
}

// TODO: Code repetition on CounterArrayIncrementAll, CounterArrayIncrementSub, etc.
func (crdt *CounterArrayCrdt) getState(updsNotYetApplied []UpdateArguments) (state CounterArrayState) {
	tmpCopy := copyToNewInt64Slice(crdt.counts)
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return CounterArrayState(tmpCopy)
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterArrayIncrement:
			tmpCopy[typedUpd.Position] += typedUpd.Change
		case CounterArrayDecrement:
			tmpCopy[typedUpd.Position] -= typedUpd.Change
		case CounterArrayIncrementAll:
			typedValue := int64(typedUpd)
			for i := range tmpCopy {
				tmpCopy[i] += typedValue
			}
		case CounterArrayDecrementAll:
			typedValue := int64(typedUpd)
			for i := range tmpCopy {
				tmpCopy[i] -= typedValue
			}
		case CounterArrayIncrementMulti:
			if len(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewInt64SliceWithSize(tmpCopy, len(typedUpd))
			}
			for i, change := range typedUpd {
				tmpCopy[i] += change
			}
		case CounterArrayDecrementMulti:
			if len(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewInt64SliceWithSize(tmpCopy, len(typedUpd))
			}
			for i, change := range typedUpd {
				tmpCopy[i] -= change
			}
		case CounterArrayIncrementSub:
			if len(typedUpd.Changes) == 1 {
				change := typedUpd.Changes[0]
				for _, pos := range typedUpd.Positions {
					tmpCopy[pos] += change
				}
			} else {
				for i, pos := range typedUpd.Positions {
					tmpCopy[pos] += typedUpd.Changes[i]
				}
			}
		case CounterArrayDecrementSub:
			if len(typedUpd.Changes) == 1 {
				change := typedUpd.Changes[0]
				for _, pos := range typedUpd.Positions {
					tmpCopy[pos] -= change
				}
			} else {
				for i, pos := range typedUpd.Positions {
					tmpCopy[pos] -= typedUpd.Changes[i]
				}
			}
		case CounterArraySetSize:
			if int(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewInt64SliceWithSize(tmpCopy, int(typedUpd))
			}
		}
	}
	return CounterArrayState(tmpCopy)
}

func (crdt *CounterArrayCrdt) getExceptRangeState(updsNotYetApplied []UpdateArguments, exceptRange []int32,
	nPosSkip int32) (state CounterArrayState) {
	sourceSlice := crdt.counts
	if len(updsNotYetApplied) > 0 {
		//First, copy the whole slice (sigh). Then, apply updates and finally filter.
		sourceSlice = crdt.getState(updsNotYetApplied) //Copies and applies updates
	}
	result := make([]int64, len(sourceSlice)-int(nPosSkip))
	//First copy the state, then apply any pending updates.
	currStop, nextStart, resultI := int32(0), int32(0), 0
	//exceptRange: Evens: start. Odds: end. So we read until "start" (except), and then we start one position in front of "end".
	for i := 0; i < len(exceptRange); i += 2 {
		currStop = exceptRange[i]
		//fmt.Printf("[CounterArrayCrdt]Except range: [%d - %d]. Writing from %d to %d (inclusive)\n", exceptRange[i], exceptRange[i+1], nextStart, currStop-1)
		for j := nextStart; j < currStop; j++ {
			result[resultI] = sourceSlice[j]
			resultI++
		}
		nextStart = exceptRange[i+1] + 1 //Start one position ahead of the end (as the except range is inclusive on both ends)
	}
	//Need to copy the rest (i.e., after the last except)
	//fmt.Printf("[CounterArrayCrdt]Writting the rest from %d to %d\n", nextStart, len(sourceSlice)-1)
	for i := int(nextStart); i < len(sourceSlice); i++ {
		result[resultI] = sourceSlice[i]
		resultI++
	}
	result = result[:resultI]
	return CounterArrayState(result)
}

func (crdt *CounterArrayCrdt) getExceptState(updsNotYetApplied []UpdateArguments, exceptPos int32) (state State) {
	result := make([]int64, len(crdt.counts)-1)
	copy(result[:exceptPos], crdt.counts[:exceptPos])
	copy(result[exceptPos:], crdt.counts[exceptPos+1:])
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return CounterArrayState(result)
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterArrayIncrement:
			if typedUpd.Position < exceptPos {
				result[typedUpd.Position] += typedUpd.Change
			} else if typedUpd.Position > exceptPos {
				result[typedUpd.Position-1] += typedUpd.Change
			}
		case CounterArrayDecrement:
			if typedUpd.Position < exceptPos {
				result[typedUpd.Position] -= typedUpd.Change
			} else if typedUpd.Position > exceptPos {
				result[typedUpd.Position-1] -= typedUpd.Change
			}
		case CounterArrayIncrementAll:
			typedValue := int64(typedUpd)
			for i := range result {
				result[i] += typedValue
			}
		case CounterArrayDecrementAll:
			typedValue := int64(typedUpd)
			for i := range result {
				result[i] -= typedValue
			}
		case CounterArrayIncrementMulti:
			if len(typedUpd) > len(result) {
				result = copyToNewInt64SliceWithSize(result, len(typedUpd)-1) //Take away the space for the except
			}
			exceptPosInt := int(exceptPos)
			for i, change := range typedUpd[:exceptPos] {
				result[i] += change
			}
			for i, change := range typedUpd[exceptPos+1:] {
				result[i+exceptPosInt] += change
			}
		case CounterArrayDecrementMulti:
			if len(typedUpd) > len(result) {
				result = copyToNewInt64SliceWithSize(result, len(typedUpd)-1) //Take away the space for the except
			}
			exceptPosInt := int(exceptPos)
			for i, change := range typedUpd[:exceptPos] {
				result[i] -= change
			}
			for i, change := range typedUpd[exceptPos+1:] {
				result[i+exceptPosInt] -= change
			}
		case CounterArrayIncrementSub:
			if len(typedUpd.Changes) == 1 {
				change := typedUpd.Changes[0]
				for _, pos := range typedUpd.Positions {
					if pos < exceptPos {
						result[pos] += change
					} else if pos > exceptPos {
						result[pos-1] += change
					}
				}
			} else {
				for i, pos := range typedUpd.Positions {
					if pos < exceptPos {
						result[pos] += typedUpd.Changes[i]
					} else if pos > exceptPos {
						result[pos-1] += typedUpd.Changes[i]
					}
				}
			}
		case CounterArrayDecrementSub:
			if len(typedUpd.Changes) == 1 {
				change := typedUpd.Changes[0]
				for _, pos := range typedUpd.Positions {
					if pos < exceptPos {
						result[pos] -= change
					} else if pos > exceptPos {
						result[pos-1] -= change
					}
				}
			} else {
				for i, pos := range typedUpd.Positions {
					if pos < exceptPos {
						result[pos] -= typedUpd.Changes[i]
					} else if pos > exceptPos {
						result[pos-1] -= typedUpd.Changes[i]
					}
				}
			}
		case CounterArraySetSize:
			if int(typedUpd) > len(result)+1 {
				result = copyToNewInt64SliceWithSize(result, int(typedUpd)-1) //Take away the space for the except
			}
		}
	}
	return CounterArrayState(result)
}

func (crdt *CounterArrayCrdt) getSingleState(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return CounterArraySingleState(crdt.counts[pos])
	}
	var value int64
	if pos > int32(len(crdt.counts)) {
		value = 0
	} else {
		value = crdt.counts[pos]
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterArrayIncrement:
			if typedUpd.Position == pos {
				value += typedUpd.Change
			}
		case CounterArrayDecrement:
			if typedUpd.Position == pos {
				value -= typedUpd.Change
			}
		case CounterArrayIncrementAll:
			value += int64(typedUpd)
		case CounterArrayDecrementAll:
			value -= int64(typedUpd)
		case CounterArrayIncrementMulti:
			if len(typedUpd) > int(pos) { //Then this position is being changed by multi
				value += typedUpd[pos]
			}
		case CounterArrayDecrementMulti:
			if len(typedUpd) > int(pos) { //Then this position is being changed by multi
				value -= typedUpd[pos]
			}
		case CounterArrayIncrementSub:
			for i, subPos := range typedUpd.Positions {
				if subPos == pos {
					if len(typedUpd.Changes) == 1 {
						value += typedUpd.Changes[0]
					} else {
						value += typedUpd.Changes[i]
					}
					break
				}
			}
		case CounterArrayDecrementSub:
			for i, subPos := range typedUpd.Positions {
				if subPos == pos {
					if len(typedUpd.Changes) == 1 {
						value -= typedUpd.Changes[0]
					} else {
						value -= typedUpd.Changes[i]
					}
					break
				}
			}
		}
	}
	return CounterArraySingleState(value)
}

func (crdt *CounterArrayCrdt) getSubState(updsNotYetApplied []UpdateArguments, positions []int32) (state State) {
	result := make([]int64, len(positions))
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		for i, pos := range positions {
			if pos > int32(len(crdt.counts)) {
				result[i] = 0
			} else {
				result[i] = crdt.counts[pos]
			}
		}
		return CounterArrayState(result)
	}
	tmpCopy := copyToNewInt64Slice(crdt.counts)
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterArrayIncrement: //Inneficient: Worth thinking of a better solution.
			/*for i, pos := range positions {
				if pos == typedUpd.Position {
					result[i] += typedUpd.Change
					break
				}
			}*/
			tmpCopy[typedUpd.Position] += typedUpd.Change
		case CounterArrayDecrement: //Inneficient: Worth thinking of a better solution.
			/*for i, pos := range positions {
				if pos == typedUpd.Position {
					result[i] -= typedUpd.Change
					break
				}
			}*/
			tmpCopy[typedUpd.Position] -= typedUpd.Change
		case CounterArrayIncrementAll:
			value := int64(typedUpd)
			/*for i := range positions {
				result[i] += value
			}*/
			for _, pos := range positions {
				tmpCopy[pos] += value
			}
		case CounterArrayDecrementAll:
			value := int64(typedUpd)
			/*for i := range positions {
				result[i] -= value
			}*/
			for _, pos := range positions {
				tmpCopy[pos] -= value
			}
		case CounterArrayIncrementMulti:
			if len(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewInt64SliceWithSize(tmpCopy, len(typedUpd))
			}
			for i, change := range typedUpd {
				tmpCopy[i] += change
			}
		case CounterArrayDecrementMulti:
			if len(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewInt64SliceWithSize(tmpCopy, len(typedUpd))
			}
			for i, change := range typedUpd {
				tmpCopy[i] -= change
			}

		case CounterArrayIncrementSub:
			if len(typedUpd.Changes) == 1 {
				change := typedUpd.Changes[0]
				for _, pos := range typedUpd.Positions {
					tmpCopy[pos] += change
				}
			} else {
				for i, pos := range typedUpd.Positions {
					tmpCopy[pos] += typedUpd.Changes[i]
				}
			}
		case CounterArrayDecrementSub:
			if len(typedUpd.Changes) == 1 {
				change := typedUpd.Changes[0]
				for _, pos := range typedUpd.Positions {
					tmpCopy[pos] -= change
				}
			} else {
				for i, pos := range typedUpd.Positions {
					tmpCopy[pos] -= typedUpd.Changes[i]
				}
			}
		case CounterArraySetSize:
			if int(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewInt64SliceWithSize(tmpCopy, int(typedUpd))
			}
		}
	}
	for i, pos := range positions {
		if pos > int32(len(tmpCopy)) {
			result[i] = 0
		} else {
			result[i] = tmpCopy[pos]
		}
	}
	return CounterArrayState(result)
}

func (crdt *CounterArrayCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	if typedArg, ok := args.(CounterArraySetSize); ok {
		if int(typedArg) <= len(crdt.counts) {
			return NoOp{} //New set size is smaller than the current size... so useless operation.
		}
	}
	return args.(DownstreamArguments)
}

func (crdt *CounterArrayCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

// Pre: newSize > len(crdt.counts)
func (crdt *CounterArrayCrdt) expandArray(newSize int32) {
	newCounts := make([]int64, newSize)
	copy(newCounts, crdt.counts)
	crdt.counts = newCounts
}

func (crdt *CounterArrayCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	switch typedUpd := downstreamArgs.(type) {
	case CounterArrayIncrement:
		if int(typedUpd.Position) >= len(crdt.counts) {
			effectValue = CounterArraySetSizeEffect(len(crdt.counts)) //The value of this position before was "0" as it did not belong to the array
			crdt.expandArray(typedUpd.Position + 1)
		} else {
			effectValue = CounterArrayIncrementEffect(typedUpd)
		}
		crdt.counts[typedUpd.Position] += typedUpd.Change
	case CounterArrayDecrement:
		if int(typedUpd.Position) >= len(crdt.counts) {
			effectValue = CounterArraySetSizeEffect(len(crdt.counts))
			crdt.expandArray(typedUpd.Position + 1)
		} else {
			effectValue = CounterArrayDecrementEffect(typedUpd)
		}
		crdt.counts[typedUpd.Position] -= typedUpd.Change
	case CounterArrayIncrementAll:
		effectValue = CounterArrayIncrementAllEffect(typedUpd)
		typedChange := int64(typedUpd)
		for i := range crdt.counts {
			crdt.counts[i] += typedChange
		}
	case CounterArrayDecrementAll:
		effectValue = CounterArrayDecrementAllEffect(typedUpd)
		typedChange := int64(typedUpd)
		for i := range crdt.counts {
			crdt.counts[i] -= typedChange
		}
	case CounterArrayIncrementMulti:
		if len(typedUpd) > len(crdt.counts) {
			effectValue = CounterArrayIncMultiWithSizeEffect{IncEff: CounterArrayIncrementMultiEffect(typedUpd), OldSize: len(crdt.counts)}
			crdt.expandArray(int32(len(typedUpd)))
		} else {
			effectValue = CounterArrayIncrementMultiEffect(typedUpd)
		}
		for i, change := range typedUpd {
			crdt.counts[i] += change
		}
	case CounterArrayDecrementMulti:
		if len(typedUpd) > len(crdt.counts) {
			effectValue = CounterArrayDecMultiWithSizeEffect{DecEff: CounterArrayDecrementMultiEffect(typedUpd), OldSize: len(crdt.counts)}
			crdt.expandArray(int32(len(typedUpd)))
		} else {
			effectValue = CounterArrayIncrementMultiEffect(typedUpd)
		}
		for i, change := range typedUpd {
			crdt.counts[i] += change
		}

	case CounterArrayIncrementSub:
		oldSize := len(crdt.counts)
		if len(typedUpd.Changes) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Positions {
				if int(pos) >= len(crdt.counts) {
					crdt.expandArray(pos + 1)
				}
				crdt.counts[pos] += change
			}
		} else {
			for i, pos := range typedUpd.Positions {
				if int(pos) >= len(crdt.counts) {
					crdt.expandArray(pos + 1)
				}
				if pos == -1 {
					fmt.Printf("[CounterArrayCrdt][ERROR]Position is -1. Changes: %v. Positions: %v\n", typedUpd.Changes, typedUpd.Positions)
				}
				crdt.counts[pos] += typedUpd.Changes[i]
			}
		}
		if oldSize != len(crdt.counts) {
			effectValue = CounterArrayIncSubWithSizeEffect{IncEff: CounterArrayIncrementSubEffect(typedUpd), OldSize: oldSize}
		} else {
			effectValue = CounterArrayIncrementSubEffect(typedUpd)
		}
	case CounterArrayDecrementSub:
		oldSize := len(crdt.counts)
		if len(typedUpd.Changes) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Positions {
				if int(pos) >= len(crdt.counts) {
					crdt.expandArray(pos + 1)
				}
				crdt.counts[pos] -= change
			}
		} else {
			for i, pos := range typedUpd.Positions {
				if int(pos) >= len(crdt.counts) {
					crdt.expandArray(pos + 1)
				}
				crdt.counts[pos] -= typedUpd.Changes[i]
			}
		}
		if oldSize != len(crdt.counts) {
			effectValue = CounterArrayDecSubWithSizeEffect{DecEff: CounterArrayDecrementSubEffect(typedUpd), OldSize: oldSize}
		} else {
			effectValue = CounterArrayDecrementSubEffect(typedUpd)
		}
	case CounterArraySetSize:
		if int(typedUpd) > len(crdt.counts) {
			effectValue = CounterArraySetSizeEffect(len(crdt.counts))
			crdt.expandArray(int32(typedUpd))
		} else { //Can still happen (e.g., two concurrent set sizes)
			effectValue = NoEffect{}
		}
	}
	return &effectValue
}

func (crdt *CounterArrayCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *CounterArrayCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := CounterArrayCrdt{CRDTVM: crdt.CRDTVM.copy(), counts: copyToNewInt64Slice(crdt.counts)}
	return &newCRDT
}

func (crdt *CounterArrayCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Most likely can do a small optimization to the one possible for Counters.
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *CounterArrayCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *CounterArrayCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case CounterArrayIncrementEffect:
		crdt.counts[typedEffect.Position] -= typedEffect.Change
	case CounterArrayDecrementEffect:
		crdt.counts[typedEffect.Position] += typedEffect.Change
	case CounterArrayIncrementAllEffect:
		typedChange := int64(typedEffect)
		for i := range crdt.counts {
			crdt.counts[i] -= typedChange
		}
	case CounterArrayDecrementAllEffect:
		typedChange := int64(typedEffect)
		for i := range crdt.counts {
			crdt.counts[i] += typedChange
		}
	case CounterArrayIncrementMultiEffect:
		for i, change := range typedEffect {
			crdt.counts[i] -= change
		}
	case CounterArrayDecrementMultiEffect:
		for i, change := range typedEffect {
			crdt.counts[i] += change
		}
	case CounterArrayIncrementSubEffect:
		if len(typedEffect.Changes) == 1 {
			change := typedEffect.Changes[0]
			for _, pos := range typedEffect.Positions {
				crdt.counts[pos] -= change
			}
		} else {
			for i, pos := range typedEffect.Positions {
				crdt.counts[pos] -= typedEffect.Changes[i]
			}
		}
	case CounterArrayDecrementSubEffect:
		if len(typedEffect.Changes) == 1 {
			change := typedEffect.Changes[0]
			for _, pos := range typedEffect.Positions {
				crdt.counts[pos] += change
			}
		} else {
			for i, pos := range typedEffect.Positions {
				crdt.counts[pos] += typedEffect.Changes[i]
			}
		}
	case CounterArrayIncMultiWithSizeEffect:
		for i, change := range typedEffect.IncEff {
			crdt.counts[i] -= change
		}
		crdt.counts = crdt.counts[:typedEffect.OldSize]
	case CounterArrayDecMultiWithSizeEffect:
		for i, change := range typedEffect.DecEff {
			crdt.counts[i] += change
		}
		crdt.counts = crdt.counts[:typedEffect.OldSize]
	case CounterArrayIncSubWithSizeEffect:
		if len(typedEffect.IncEff.Changes) == 1 {
			change := typedEffect.IncEff.Changes[0]
			for _, pos := range typedEffect.IncEff.Positions {
				crdt.counts[pos] -= change
			}
		} else {
			for i, pos := range typedEffect.IncEff.Positions {
				crdt.counts[pos] -= typedEffect.IncEff.Changes[i]
			}
		}
		crdt.counts = crdt.counts[:typedEffect.OldSize]
	case CounterArrayDecSubWithSizeEffect:
		if len(typedEffect.DecEff.Changes) == 1 {
			change := typedEffect.DecEff.Changes[0]
			for _, pos := range typedEffect.DecEff.Positions {
				crdt.counts[pos] += change
			}
		} else {
			for i, pos := range typedEffect.DecEff.Positions {
				crdt.counts[pos] += typedEffect.DecEff.Changes[i]
			}
		}
		crdt.counts = crdt.counts[:typedEffect.OldSize]
	case CounterArraySetSizeEffect:
		crdt.counts = crdt.counts[:typedEffect]
	}
}

func (crdt *CounterArrayCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

func copyToNewInt64Slice(slice []int64) []int64 {
	newSlice := make([]int64, len(slice))
	copy(newSlice, slice)
	return newSlice
}

func copyToNewInt64SliceWithSize(slice []int64, sizeOfNewSlice int) []int64 {
	newSlice := make([]int64, sizeOfNewSlice)
	copy(newSlice, slice)
	return newSlice
}

//Protobuf functions

func (crdtOp CounterArrayIncrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetArraycounterop().GetInc()
	return CounterArrayIncrement{Change: incProto.GetInc(), Position: incProto.GetIndex()}
}

func (crdtOp CounterArrayIncrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{
		Inc: &proto.ApbArrayCounterIncrement{Index: pb.Int32(crdtOp.Position), Inc: pb.Int64(crdtOp.Change)}}}
}

func (crdtOp CounterArrayDecrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetArraycounterop().GetInc()
	return CounterArrayIncrement{Change: -incProto.GetInc(), Position: incProto.GetIndex()}
}

func (crdtOp CounterArrayDecrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{
		Inc: &proto.ApbArrayCounterIncrement{Index: pb.Int32(crdtOp.Position), Inc: pb.Int64(-crdtOp.Change)}}}
}

func (crdtOp CounterArrayIncrementAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return CounterArrayIncrementAll(protobuf.GetArraycounterop().GetIncAll().GetInc())
}

func (crdtOp CounterArrayIncrementAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{
		IncAll: &proto.ApbArrayCounterIncrementAll{Inc: pb.Int64(int64(crdtOp))}}}
}

func (crdtOp CounterArrayDecrementAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return CounterArrayDecrementAll(-protobuf.GetArraycounterop().GetIncAll().GetInc())
}

func (crdtOp CounterArrayDecrementAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{
		IncAll: &proto.ApbArrayCounterIncrementAll{Inc: pb.Int64(int64(-crdtOp))}}}
}

func (crdtOp CounterArrayIncrementMulti) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return CounterArrayIncrementMulti(protobuf.GetArraycounterop().GetIncMulti().GetIncs())
}

func (crdtOp CounterArrayIncrementMulti) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{
		IncMulti: &proto.ApbArrayCounterIncrementMulti{Incs: crdtOp}}}
}

func (crdtOp CounterArrayDecrementMulti) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	protoIncs := protobuf.GetArraycounterop().GetIncMulti().GetIncs()
	crdtOp = make([]int64, len(protoIncs))
	for i, inc := range protoIncs {
		crdtOp[i] = -inc
	}
	return crdtOp
}

func (crdtOp CounterArrayDecrementMulti) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	protoIncs := make([]int64, len(crdtOp))
	for i, inc := range crdtOp {
		protoIncs[i] = -inc
	}
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{
		IncMulti: &proto.ApbArrayCounterIncrementMulti{Incs: protoIncs}}}
}

func (crdtOp CounterArrayIncrementSub) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incSubProto := protobuf.GetArraycounterop().GetIncSub()
	return CounterArrayIncrementSub{Changes: incSubProto.GetIncs(), Positions: incSubProto.GetIndexes()}
}

func (crdtOp CounterArrayIncrementSub) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{
		IncSub: &proto.ApbArrayCounterIncrementSub{Indexes: crdtOp.Positions, Incs: crdtOp.Changes}}}
}

func (crdtOp CounterArrayDecrementSub) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incSubProto := protobuf.GetArraycounterop().GetIncSub()
	protoIncs := incSubProto.GetIncs()
	crdtOp.Changes, crdtOp.Positions = make([]int64, len(protoIncs)), incSubProto.GetIndexes()
	for i, inc := range protoIncs {
		crdtOp.Changes[i] = -inc
	}
	return crdtOp
}

func (crdtOp CounterArrayDecrementSub) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	protoIncs := make([]int64, len(crdtOp.Changes))
	for i, inc := range crdtOp.Changes {
		protoIncs[i] = -inc
	}
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{
		IncSub: &proto.ApbArrayCounterIncrementSub{Indexes: crdtOp.Positions, Incs: protoIncs}}}
}

func (crdtOp CounterArraySetSize) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return CounterArraySetSize(protobuf.GetArraycounterop().GetSize().GetSize())
}

func (crdtOp CounterArraySetSize) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Arraycounterop: &proto.ApbArrayCounterUpdate{Size: &proto.ApbArrayCounterSetSize{Size: pb.Int32(int32(crdtOp))}}}
}

func (crdtState CounterArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CounterArrayState(protobuf.GetArraycounter().GetValues())
}

func (crdtState CounterArrayState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Arraycounter: &proto.ApbGetArrayCounterResp{Values: crdtState}}
}

func (crdtState CounterArraySingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CounterArraySingleState(protobuf.GetPartread().GetArraycounter().GetValue())
}

func (crdtState CounterArraySingleState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Arraycounter: &proto.ApbArrayCounterPartialReadResp{Value: pb.Int64(int64(crdtState))}}}
}

func (args CounterArraySingleArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args = CounterArraySingleArguments(protobuf.GetArraycounter().GetSingle().GetIndex())
	return args
}

func (args CounterArraySingleArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Arraycounter: &proto.ApbArrayCounterPartialRead{Single: &proto.ApbArrayCounterSingleRead{Index: pb.Int32(int32(args))}}}
}

func (args CounterArraySubArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args = CounterArraySubArguments(protobuf.GetArraycounter().GetSub().GetIndexes())
	return args
}

func (args CounterArraySubArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Arraycounter: &proto.ApbArrayCounterPartialRead{Sub: &proto.ApbArrayCounterSubRead{Indexes: args}}}
}

func (args CounterArrayExceptArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args = CounterArrayExceptArguments(protobuf.GetArraycounter().GetExcept().GetIndex())
	return args
}

func (args CounterArrayExceptArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Arraycounter: &proto.ApbArrayCounterPartialRead{Except: &proto.ApbArrayCounterExceptRead{Index: pb.Int32(int32(args))}}}
}

func (args CounterArrayExceptRangeArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	rangeProto := protobuf.GetArraycounter().GetExceptRange()
	return CounterArrayExceptRangeArguments{ExceptRange: rangeProto.GetIndexes(), NPositionsSkip: rangeProto.GetNPositionsSkip()}
}

func (args CounterArrayExceptRangeArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Arraycounter: &proto.ApbArrayCounterPartialRead{ExceptRange: &proto.ApbArrayCounterExceptRangeRead{Indexes: args.ExceptRange, NPositionsSkip: pb.Int32(args.NPositionsSkip)}}}
}

func (downOp CounterArrayIncrement) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	incProto := protobuf.GetArrayCounterOp().GetInc()
	return CounterArrayIncrement{Change: incProto.GetInc(), Position: incProto.GetIndex()}
}

func (downOp CounterArrayIncrement) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{
		IsInc: pb.Bool(true), Inc: &proto.ProtoArrayCounterIncrementDownstream{Index: pb.Int32(downOp.Position), Inc: pb.Int64(downOp.Change)}}}
}

func (downOp CounterArrayDecrement) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	incProto := protobuf.GetArrayCounterOp().GetInc()
	return CounterArrayIncrement{Change: -incProto.GetInc(), Position: incProto.GetIndex()}
}

func (downOp CounterArrayDecrement) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{
		IsInc: pb.Bool(false), Inc: &proto.ProtoArrayCounterIncrementDownstream{Index: pb.Int32(downOp.Position), Inc: pb.Int64(-downOp.Change)}}}
}

func (downOp CounterArrayIncrementAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return CounterArrayIncrementAll(protobuf.GetArrayCounterOp().GetIncAll().GetInc())
}

func (downOp CounterArrayIncrementAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{
		IsInc: pb.Bool(true), IncAll: &proto.ProtoArrayCounterIncrementAllDownstream{Inc: pb.Int64(int64(downOp))}}}
}

func (downOp CounterArrayDecrementAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return CounterArrayDecrementAll(protobuf.GetArrayCounterOp().GetIncAll().GetInc())
}

func (downOp CounterArrayDecrementAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{
		IsInc: pb.Bool(false), IncAll: &proto.ProtoArrayCounterIncrementAllDownstream{Inc: pb.Int64(int64(-downOp))}}}
}

func (downOp CounterArrayIncrementMulti) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return CounterArrayIncrementMulti(protobuf.GetArrayCounterOp().GetIncMulti().GetIncs())
}

func (downOp CounterArrayIncrementMulti) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{
		IsInc: pb.Bool(true), IncMulti: &proto.ProtoArrayCounterIncrementMultiDownstream{Incs: downOp}}}
}

func (downOp CounterArrayDecrementMulti) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	protoIncs := protobuf.GetArrayCounterOp().GetIncMulti().GetIncs()
	downOp = make([]int64, len(protoIncs))
	for i, inc := range protoIncs {
		downOp[i] = -inc
	}
	return downOp
}

func (downOp CounterArrayDecrementMulti) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoIncs := make([]int64, len(downOp))
	for i, inc := range downOp {
		protoIncs[i] = -inc
	}
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{
		IsInc: pb.Bool(false), IncMulti: &proto.ProtoArrayCounterIncrementMultiDownstream{Incs: protoIncs}}}
}

func (downOp CounterArrayIncrementSub) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	incSubProto := protobuf.GetArrayCounterOp().GetIncSub()
	return CounterArrayIncrementSub{Changes: incSubProto.GetIncs(), Positions: incSubProto.GetIndexes()}
}

func (downOp CounterArrayIncrementSub) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{
		IsInc: pb.Bool(true), IncSub: &proto.ProtoArrayCounterIncrementSubDownstream{Indexes: downOp.Positions, Incs: downOp.Changes}}}
}

func (downOp CounterArrayDecrementSub) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	incSubProto := protobuf.GetArrayCounterOp().GetIncSub()
	protoIncs := incSubProto.GetIncs()
	downOp.Changes, downOp.Positions = make([]int64, len(protoIncs)), incSubProto.GetIndexes()
	for i, inc := range protoIncs {
		downOp.Changes[i] = -inc
	}
	return downOp
}

func (downOp CounterArrayDecrementSub) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoIncs := make([]int64, len(downOp.Changes))
	for i, inc := range downOp.Changes {
		protoIncs[i] = -inc
	}
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{
		IsInc: pb.Bool(false), IncSub: &proto.ProtoArrayCounterIncrementSubDownstream{Indexes: downOp.Positions, Incs: protoIncs}}}
}

func (downOp CounterArraySetSize) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return CounterArraySetSize(protobuf.GetArrayCounterOp().GetSize().GetSize())
}

func (downOp CounterArraySetSize) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{ArrayCounterOp: &proto.ProtoArrayCounterDownstream{Size: &proto.ProtoArraySetSize{Size: pb.Int32(int32(downOp))}}}
}

func (crdt *CounterArrayCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{ArrayCounter: &proto.ProtoArrayCounterState{Counts: crdt.counts}}
}

func (crdt *CounterArrayCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	return (&CounterArrayCrdt{counts: proto.GetArrayCounter().GetCounts()}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *CounterArrayCrdt) GetCRDT() CRDT { return crdt }
