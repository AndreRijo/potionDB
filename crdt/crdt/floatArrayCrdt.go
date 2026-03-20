package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"

	tools "github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

// Disclaimer: this is a copy of FloatArrayCrdt, but adapted for floats. There may be a lot of "bad variable names" or repeated comments.

// IMPORTANT NOTE: FloatArraySetSize must be used before any increment or decrement operations,
// or when it is known that no concurrent operations will be happening.
// While FloatArraySetSize commutes with FloatArrayIncrement/Decrement and
// FloatArrayIncrement/DecrementSub, it does not with FloatArrayIncrement/DecrementAll
// If one must be able to use FloatArraySetSize concurrently with inc/dec, in order to ensure correctness
// do not use FloatArrayIncrement/DecrementAll.
// FloatArraySetSize does commute with FloatArrayIncrement/DecrementMulti, as follows.
// It will only increment/decrement as many positions as the values in the operation, and grows the array
// as needed. If the array size is >= len(Changes), due to a concurrent FloatArraySetSize, the extra positions
// will remain unchanged.
type FloatArrayCrdt struct {
	CRDTVM
	values []float64
}

//States

type FloatArrayCRDTState []float64

type FloatArrayCRDTSingleState float64

//Reads

// Position
type FloatArraySingleArguments int32
type FloatArrayExceptArguments int32
type FloatArrayRangeArguments struct { // Note: [From:To], as in Go slices, i.e., excluding To.
	From, To int32
}

// Positions
type FloatArraySubArguments []int32

type FloatArrayExceptRangeArguments struct {
	ExceptRange    []int32 //Ranges to skip. Evens: start. Odds: end.
	NPositionsSkip int32   //Optional. This helps to create a read slice with a more appropriate dimension.
}

//Updates

type FloatArraySetSize int32

type FloatArrayIncrement struct {
	Change   float64
	Position int32
}

type FloatArrayDecrement struct {
	Change   float64
	Position int32
}

// If len(Changes) == 1, then increments all positions in Positions by Changes[0]
// Otherwise, assumes len(changes) == len(positions)
type FloatArrayIncrementSub struct {
	Changes   []float64
	Positions []int32
}

type FloatArrayDecrementSub struct {
	Changes   []float64
	Positions []int32
}

type FloatArrayIncrementAll float64
type FloatArrayDecrementAll float64

// []float64: Changes.
type FloatArrayIncrementMulti []float64
type FloatArrayDecrementMulti []float64

// Excludes To, just like in Go's slices.
type FloatArrayIncrementRange struct {
	Change   float64
	From, To int32
}
type FloatArrayDecrementRange struct {
	Change   float64
	From, To int32
}

type FloatArrayIncrementEffect FloatArrayIncrement
type FloatArrayDecrementEffect FloatArrayDecrement
type FloatArrayIncrementSubEffect FloatArrayIncrementSub
type FloatArrayDecrementSubEffect FloatArrayDecrementSub
type FloatArrayIncrementAllEffect FloatArrayIncrementAll
type FloatArrayDecrementAllEffect FloatArrayDecrementAll
type FloatArrayIncrementMultiEffect FloatArrayIncrementMulti
type FloatArrayDecrementMultiEffect FloatArrayDecrementMulti
type FloatArrayIncrementRangeEffect FloatArrayIncrementRange
type FloatArrayDecrementRangeEffect FloatArrayDecrementRange
type FloatArraySetSizeEffect FloatArraySetSize

type FloatArrayIncSubWithSizeEffect struct {
	IncEff  FloatArrayIncrementSubEffect
	OldSize int
}
type FloatArrayDecSubWithSizeEffect struct {
	DecEff  FloatArrayDecrementSubEffect
	OldSize int
}
type FloatArrayIncMultiWithSizeEffect struct {
	IncEff  FloatArrayIncrementMultiEffect
	OldSize int
}
type FloatArrayDecMultiWithSizeEffect struct {
	DecEff  FloatArrayDecrementMultiEffect
	OldSize int
}
type FloatArrayIncRangeWithSizeEffect struct {
	IncEff  FloatArrayIncrementRangeEffect
	OldSize int
}
type FloatArrayDecRangeWithSizeEffect struct {
	DecEff  FloatArrayDecrementRangeEffect
	OldSize int
}

func (crdt *FloatArrayCrdt) GetCRDTType() proto.CRDTType          { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArraySetSize) GetCRDTType() proto.CRDTType        { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayIncrement) GetCRDTType() proto.CRDTType      { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayDecrement) GetCRDTType() proto.CRDTType      { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayIncrementSub) GetCRDTType() proto.CRDTType   { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayDecrementSub) GetCRDTType() proto.CRDTType   { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayIncrementAll) GetCRDTType() proto.CRDTType   { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayDecrementAll) GetCRDTType() proto.CRDTType   { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayIncrementMulti) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayDecrementMulti) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayIncrementRange) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayDecrementRange) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_FLOAT }
func (crdt *FloatArrayCrdt) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }
func (args FloatArraySetSize) GetDATAType() proto.DATAType        { return proto.DATAType_DEFAULT }
func (args FloatArrayIncrement) GetDATAType() proto.DATAType      { return proto.DATAType_DEFAULT }
func (args FloatArrayDecrement) GetDATAType() proto.DATAType      { return proto.DATAType_DEFAULT }
func (args FloatArrayIncrementSub) GetDATAType() proto.DATAType   { return proto.DATAType_DEFAULT }
func (args FloatArrayDecrementSub) GetDATAType() proto.DATAType   { return proto.DATAType_DEFAULT }
func (args FloatArrayIncrementAll) GetDATAType() proto.DATAType   { return proto.DATAType_DEFAULT }
func (args FloatArrayDecrementAll) GetDATAType() proto.DATAType   { return proto.DATAType_DEFAULT }
func (args FloatArrayIncrementMulti) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args FloatArrayDecrementMulti) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args FloatArrayIncrementRange) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args FloatArrayDecrementRange) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

func (state FloatArrayCRDTState) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_FLOAT }
func (state FloatArrayCRDTState) GetREADType() proto.READType { return proto.READType_FULL }
func (state FloatArrayCRDTState) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state FloatArrayCRDTSingleState) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_FLOAT
}
func (state FloatArrayCRDTSingleState) GetREADType() proto.READType {
	return proto.READType_FLOAT_SINGLE
}
func (state FloatArrayCRDTSingleState) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args FloatArraySetSize) MustReplicate() bool                  { return true }
func (args FloatArrayIncrement) MustReplicate() bool                { return true }
func (args FloatArrayDecrement) MustReplicate() bool                { return true }
func (args FloatArrayIncrementSub) MustReplicate() bool             { return true }
func (args FloatArrayDecrementSub) MustReplicate() bool             { return true }
func (args FloatArrayIncrementAll) MustReplicate() bool             { return true }
func (args FloatArrayDecrementAll) MustReplicate() bool             { return true }
func (args FloatArrayIncrementMulti) MustReplicate() bool           { return true }
func (args FloatArrayDecrementMulti) MustReplicate() bool           { return true }
func (args FloatArrayIncrementRange) MustReplicate() bool           { return true }
func (args FloatArrayDecrementRange) MustReplicate() bool           { return true }
func (args FloatArraySingleArguments) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArraySubArguments) GetCRDTType() proto.CRDTType     { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayRangeArguments) GetCRDTType() proto.CRDTType   { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayExceptArguments) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_FLOAT }
func (args FloatArrayExceptRangeArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_FLOAT
}
func (args FloatArraySingleArguments) GetREADType() proto.READType {
	return proto.READType_FLOAT_SINGLE
}
func (args FloatArraySubArguments) GetREADType() proto.READType   { return proto.READType_FLOAT_SUB }
func (args FloatArrayRangeArguments) GetREADType() proto.READType { return proto.READType_FLOAT_RANGE }
func (args FloatArrayExceptArguments) GetREADType() proto.READType {
	return proto.READType_FLOAT_EXCEPT
}
func (args FloatArrayExceptRangeArguments) GetREADType() proto.READType {
	return proto.READType_FLOAT_EXCEPT_RANGE
}
func (args FloatArraySingleArguments) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args FloatArraySubArguments) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }
func (args FloatArrayRangeArguments) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }
func (args FloatArrayExceptArguments) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args FloatArrayExceptRangeArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args FloatArraySingleArguments) HasInnerReads() bool      { return false }
func (args FloatArraySubArguments) HasInnerReads() bool         { return false }
func (args FloatArrayRangeArguments) HasInnerReads() bool       { return false }
func (args FloatArrayExceptArguments) HasInnerReads() bool      { return false }
func (args FloatArrayExceptRangeArguments) HasInnerReads() bool { return false }
func (args FloatArraySingleArguments) HasVariables() bool       { return false }
func (args FloatArraySubArguments) HasVariables() bool          { return false }
func (args FloatArrayRangeArguments) HasVariables() bool        { return false }
func (args FloatArrayExceptArguments) HasVariables() bool       { return false }
func (args FloatArrayExceptRangeArguments) HasVariables() bool  { return false }

func (crdt *FloatArrayCrdt) Initialize(startTs *clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	crdt = &FloatArrayCrdt{values: make([]float64, 1)}
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *FloatArrayCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID uint16) (sameCRDT *FloatArrayCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *FloatArrayCrdt) IsBigCRDT() bool { return len(crdt.values) >= 1000 }

func (crdt *FloatArrayCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case FloatArraySingleArguments:
		return crdt.getSingleState(updsNotYetApplied, int32(typedArgs))
	case FloatArrayExceptArguments:
		if int(typedArgs) >= len(crdt.values) { //TODO: This is not fully correct if we account for updsNotYetApplied.
			return crdt.getState(updsNotYetApplied)
		}
		return crdt.getExceptState(updsNotYetApplied, int32(typedArgs))
	case FloatArraySubArguments:
		return crdt.getSubState(updsNotYetApplied, []int32(typedArgs))
	case FloatArrayRangeArguments:
		return crdt.getRangeState(updsNotYetApplied, typedArgs.From, typedArgs.To)
	case FloatArrayExceptRangeArguments:
		return crdt.getExceptRangeState(updsNotYetApplied, typedArgs.ExceptRange, typedArgs.NPositionsSkip)
	default:
		fmt.Printf("[FloatArrayCrdt] Unknown read type: %+v\n", args)
	}
	return nil
}

// TODO: Code repetition on FloatArrayIncrementAll, FloatArrayIncrementSub, etc.
func (crdt *FloatArrayCrdt) getState(updsNotYetApplied []UpdateArguments) (state FloatArrayCRDTState) {
	tmpCopy := copyToNewFloat64Slice(crdt.values)
	if len(updsNotYetApplied) == 0 {
		return FloatArrayCRDTState(tmpCopy)
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case FloatArrayIncrement:
			tmpCopy[typedUpd.Position] += typedUpd.Change
		case FloatArrayDecrement:
			tmpCopy[typedUpd.Position] -= typedUpd.Change
		case FloatArrayIncrementAll:
			typedValue := float64(typedUpd)
			for i := range tmpCopy {
				tmpCopy[i] += typedValue
			}
		case FloatArrayDecrementAll:
			typedValue := float64(typedUpd)
			for i := range tmpCopy {
				tmpCopy[i] -= typedValue
			}
		case FloatArrayIncrementMulti:
			if len(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, len(typedUpd))
			}
			for i, change := range typedUpd {
				tmpCopy[i] += change
			}
		case FloatArrayDecrementMulti:
			if len(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, len(typedUpd))
			}
			for i, change := range typedUpd {
				tmpCopy[i] -= change
			}
		case FloatArrayIncrementSub:
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
		case FloatArrayDecrementSub:
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
		case FloatArrayIncrementRange:
			if int(typedUpd.To) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, int(typedUpd.To))
			}
			for i := typedUpd.From; i < typedUpd.To; i++ {
				tmpCopy[i] += typedUpd.Change
			}
		case FloatArrayDecrementRange:
			if int(typedUpd.To) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, int(typedUpd.To))
			}
			for i := typedUpd.From; i < typedUpd.To; i++ {
				tmpCopy[i] -= typedUpd.Change
			}
		case FloatArraySetSize:
			if int(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, int(typedUpd))
			}
		}
	}
	return FloatArrayCRDTState(tmpCopy)
}

func (crdt *FloatArrayCrdt) getExceptRangeState(updsNotYetApplied []UpdateArguments, exceptRange []int32,
	nPosSkip int32) (state FloatArrayCRDTState) {
	sourceSlice := crdt.values
	if len(updsNotYetApplied) > 0 {
		//First, copy the whole slice (sigh). Then, apply updates and finally filter.
		sourceSlice = crdt.getState(updsNotYetApplied) //Copies and applies updates
	}
	result := make([]float64, len(sourceSlice)-int(nPosSkip))
	//First copy the state, then apply any pending updates.
	currStop, nextStart, resultI := int32(0), int32(0), 0
	//exceptRange: Evens: start. Odds: end. So we read until "start" (except), and then we start one position in front of "end".
	for i := 0; i < len(exceptRange); i += 2 {
		currStop = exceptRange[i]
		//fmt.Printf("[FloatArrayCrdt]Except range: [%d - %d]. Writing from %d to %d (inclusive)\n", exceptRange[i], exceptRange[i+1], nextStart, currStop-1)
		for j := nextStart; j < currStop; j++ {
			result[resultI] = sourceSlice[j]
			resultI++
		}
		nextStart = exceptRange[i+1] + 1 //Start one position ahead of the end (as the except range is inclusive on both ends)
	}
	//Need to copy the rest (i.e., after the last except)
	//fmt.Printf("[FloatArrayCrdt]Writting the rest from %d to %d\n", nextStart, len(sourceSlice)-1)
	for i := int(nextStart); i < len(sourceSlice); i++ {
		result[resultI] = sourceSlice[i]
		resultI++
	}
	result = result[:resultI]
	return FloatArrayCRDTState(result)
}

func (crdt *FloatArrayCrdt) getExceptState(updsNotYetApplied []UpdateArguments, exceptPos int32) (state State) {
	result := make([]float64, len(crdt.values)-1)
	copy(result[:exceptPos], crdt.values[:exceptPos])
	copy(result[exceptPos:], crdt.values[exceptPos+1:])
	if len(updsNotYetApplied) == 0 {
		return FloatArrayCRDTState(result)
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case FloatArrayIncrement:
			if typedUpd.Position < exceptPos {
				result[typedUpd.Position] += typedUpd.Change
			} else if typedUpd.Position > exceptPos {
				result[typedUpd.Position-1] += typedUpd.Change
			}
		case FloatArrayDecrement:
			if typedUpd.Position < exceptPos {
				result[typedUpd.Position] -= typedUpd.Change
			} else if typedUpd.Position > exceptPos {
				result[typedUpd.Position-1] -= typedUpd.Change
			}
		case FloatArrayIncrementAll:
			typedValue := float64(typedUpd)
			for i := range result {
				result[i] += typedValue
			}
		case FloatArrayDecrementAll:
			typedValue := float64(typedUpd)
			for i := range result {
				result[i] -= typedValue
			}
		case FloatArrayIncrementMulti:
			if len(typedUpd) > len(result) {
				result = copyToNewFloat64SliceWithSize(result, len(typedUpd)-1) //Take away the space for the except
			}
			exceptPosInt := int(exceptPos)
			for i, change := range typedUpd[:exceptPos] {
				result[i] += change
			}
			for i, change := range typedUpd[exceptPos+1:] {
				result[i+exceptPosInt] += change
			}
		case FloatArrayDecrementMulti:
			if len(typedUpd) > len(result) {
				result = copyToNewFloat64SliceWithSize(result, len(typedUpd)-1) //Take away the space for the except
			}
			exceptPosInt := int(exceptPos)
			for i, change := range typedUpd[:exceptPos] {
				result[i] -= change
			}
			for i, change := range typedUpd[exceptPos+1:] {
				result[i+exceptPosInt] -= change
			}
		case FloatArrayIncrementSub:
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
		case FloatArrayDecrementSub:
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
		case FloatArrayIncrementRange:
			if int(typedUpd.To) > len(result)+1 {
				result = copyToNewFloat64SliceWithSize(result, int(typedUpd.To)-1) //Take away the space for the except
			}
			for i := typedUpd.From; i < typedUpd.To; i++ {
				if i < exceptPos {
					result[i] += typedUpd.Change
				} else if i > exceptPos {
					result[i-1] += typedUpd.Change
				}
			}
		case FloatArrayDecrementRange:
			if int(typedUpd.To) > len(result)+1 {
				result = copyToNewFloat64SliceWithSize(result, int(typedUpd.To)-1) //Take away the space for the except
			}
			for i := typedUpd.From; i < typedUpd.To; i++ {
				if i < exceptPos {
					result[i] -= typedUpd.Change
				} else if i > exceptPos {
					result[i-1] -= typedUpd.Change
				}
			}
		case FloatArraySetSize:
			if int(typedUpd) > len(result)+1 {
				result = copyToNewFloat64SliceWithSize(result, int(typedUpd)-1) //Take away the space for the except
			}
		}
	}
	return FloatArrayCRDTState(result)
}

func (crdt *FloatArrayCrdt) getSingleState(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return FloatArrayCRDTSingleState(crdt.values[pos])
	}
	var value float64
	if pos > int32(len(crdt.values)) {
		value = 0
	} else {
		value = crdt.values[pos]
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case FloatArrayIncrement:
			if typedUpd.Position == pos {
				value += typedUpd.Change
			}
		case FloatArrayDecrement:
			if typedUpd.Position == pos {
				value -= typedUpd.Change
			}
		case FloatArrayIncrementAll:
			value += float64(typedUpd)
		case FloatArrayDecrementAll:
			value -= float64(typedUpd)
		case FloatArrayIncrementMulti:
			if len(typedUpd) > int(pos) { //Then this position is being changed by multi
				value += typedUpd[pos]
			}
		case FloatArrayDecrementMulti:
			if len(typedUpd) > int(pos) { //Then this position is being changed by multi
				value -= typedUpd[pos]
			}
		case FloatArrayIncrementSub:
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
		case FloatArrayDecrementSub:
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
		case FloatArrayIncrementRange:
			if (pos >= typedUpd.From) && (pos < typedUpd.To) {
				value += typedUpd.Change
			}
		case FloatArrayDecrementRange:
			if (pos >= typedUpd.From) && (pos < typedUpd.To) {
				value -= typedUpd.Change
			}
		}
	}
	return FloatArrayCRDTSingleState(value)
}

func (crdt *FloatArrayCrdt) getRangeState(updsNotYetApplied []UpdateArguments, from int32, to int32) (state State) {
	result := getRangeOfSlice(crdt.values, int(from), int(to))
	if len(updsNotYetApplied) == 0 {
		return FloatArrayCRDTState(result)
	}
	//TODO: In this case, updates may make extra positions appear in the slice.
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case FloatArrayIncrement:
			if typedUpd.Position >= from && typedUpd.Position < to {
				result[typedUpd.Position-from] += typedUpd.Change
			}
		case FloatArrayDecrement:
			if typedUpd.Position >= from && typedUpd.Position < to {
				result[typedUpd.Position-from] -= typedUpd.Change
			}
		case FloatArrayIncrementAll:
			value := float64(typedUpd)
			for i := range result {
				result[i] += value
			}
		case FloatArrayDecrementAll:
			value := float64(typedUpd)
			for i := range result {
				result[i] -= value
			}
		case FloatArrayIncrementMulti:
			if len(typedUpd)-int(from) > len(result) && int(to-from) > len(result) {
				result = copyToNewFloat64SliceWithSize(result, int(tools.Min(int32(len(typedUpd))-from, (to-from))))
			}
			min := tools.Min(int32(len(typedUpd)), to) //In case the multi is smaller than the range
			for i := from; i < min; i++ {
				result[i-from] += typedUpd[i]
			}
		case FloatArrayDecrementMulti:
			if len(typedUpd)-int(from) > len(result) && int(to-from) > len(result) {
				result = copyToNewFloat64SliceWithSize(result, int(tools.Min(int32(len(typedUpd))-from, (to-from))))
			}
			min := tools.Min(int32(len(typedUpd)), to) //In case the multi is smaller than the range
			for i := from; i < min; i++ {
				result[i-from] -= typedUpd[i]
			}
		case FloatArrayIncrementSub:
			if len(typedUpd.Changes) == 1 {
				change := typedUpd.Changes[0]
				for _, pos := range typedUpd.Positions {
					if pos >= from && pos < to {
						result[pos-from] += change
					}
				}
			} else {
				for i, pos := range typedUpd.Positions {
					if pos >= from && pos < to {
						result[pos-from] += typedUpd.Changes[i]
					}
				}
			}
		case FloatArrayDecrementSub:
			if len(typedUpd.Changes) == 1 {
				change := typedUpd.Changes[0]
				for _, pos := range typedUpd.Positions {
					if pos >= from && pos < to {
						result[pos-from] -= change
					}
				}
			} else {
				for i, pos := range typedUpd.Positions {
					if pos >= from && pos < to {
						result[pos-from] -= typedUpd.Changes[i]
					}
				}
			}
		case FloatArrayIncrementRange:
			if int(to-from) > len(result) && typedUpd.To-from > int32(len(result)) { //Need resize.
				result = copyToNewFloat64SliceWithSize(result, int(tools.Min(typedUpd.To-from, (to-from))))
			}
			if typedUpd.From < to && typedUpd.To > from { //There is some overlap
				end := tools.Min(typedUpd.To, to) - from
				start := tools.Max(0, typedUpd.From-from) //typedUpd.From could be before from.
				for i := start; i < end; i++ {
					result[i] += typedUpd.Change
				}
			}
		case FloatArrayDecrementRange:
			if int(to-from) > len(result) && typedUpd.To-from > int32(len(result)) { //Need resize.
				result = copyToNewFloat64SliceWithSize(result, int(tools.Min(typedUpd.To-from, (to-from))))
			}
			if typedUpd.From < to && typedUpd.To > from { //There is some overlap
				end := tools.Min(typedUpd.To, to) - from
				start := tools.Max(0, typedUpd.From-from) //typedUpd.From could be before from.
				for i := start; i < end; i++ {
					result[i] += typedUpd.Change
				}
			}
		case FloatArraySetSize:
			if int(to) > len(crdt.values) && int(typedUpd) > len(result) {
				result = copyToNewFloat64SliceWithSize(result, int(tools.Min(int32(typedUpd), to)-from))
			}
		}
	}
	return FloatArrayCRDTState(result)
}

func (crdt *FloatArrayCrdt) getSubState(updsNotYetApplied []UpdateArguments, positions []int32) (state State) {
	result := make([]float64, len(positions))
	if len(updsNotYetApplied) == 0 {
		for i, pos := range positions {
			if pos >= int32(len(crdt.values)) {
				result[i] = 0
			} else {
				result[i] = crdt.values[pos]
			}
		}
		return FloatArrayCRDTState(result)
	}
	tmpCopy := copyToNewFloat64Slice(crdt.values)
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case FloatArrayIncrement: //Inneficient: Worth thinking of a better solution.
			/*for i, pos := range positions {
				if pos == typedUpd.Position {
					result[i] += typedUpd.Change
					break
				}
			}*/
			tmpCopy[typedUpd.Position] += typedUpd.Change
		case FloatArrayDecrement: //Inneficient: Worth thinking of a better solution.
			/*for i, pos := range positions {
				if pos == typedUpd.Position {
					result[i] -= typedUpd.Change
					break
				}
			}*/
			tmpCopy[typedUpd.Position] -= typedUpd.Change
		case FloatArrayIncrementAll:
			value := float64(typedUpd)
			/*for i := range positions {
				result[i] += value
			}*/
			for _, pos := range positions {
				tmpCopy[pos] += value
			}
		case FloatArrayDecrementAll:
			value := float64(typedUpd)
			/*for i := range positions {
				result[i] -= value
			}*/
			for _, pos := range positions {
				tmpCopy[pos] -= value
			}
		case FloatArrayIncrementMulti:
			if len(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, len(typedUpd))
			}
			for i, change := range typedUpd {
				tmpCopy[i] += change
			}
		case FloatArrayDecrementMulti:
			if len(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, len(typedUpd))
			}
			for i, change := range typedUpd {
				tmpCopy[i] -= change
			}

		case FloatArrayIncrementSub:
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
		case FloatArrayDecrementSub:
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
		case FloatArrayIncrementRange:
			if int(typedUpd.To) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, int(typedUpd.To))
			}
			for i := typedUpd.From; i < typedUpd.To; i++ {
				tmpCopy[i] += typedUpd.Change
			}
		case FloatArrayDecrementRange:
			if int(typedUpd.To) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, int(typedUpd.To))
			}
			for i := typedUpd.From; i < typedUpd.To; i++ {
				tmpCopy[i] -= typedUpd.Change
			}
		case FloatArraySetSize:
			if int(typedUpd) > len(tmpCopy) {
				tmpCopy = copyToNewFloat64SliceWithSize(tmpCopy, int(typedUpd))
			}
		}
	}
	for i, pos := range positions {
		if pos >= int32(len(tmpCopy)) {
			result[i] = 0
		} else {
			result[i] = tmpCopy[pos]
		}
	}
	return FloatArrayCRDTState(result)
}

func (crdt *FloatArrayCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	if typedArg, ok := args.(FloatArraySetSize); ok {
		if int(typedArg) <= len(crdt.values) {
			return NoOp{} //New set size is smaller than the current size... so useless operation.
		}
	}
	return args.(DownstreamArguments)
}

func (crdt *FloatArrayCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		for _, upd := range multiUpd {
			crdt.Downstream(updTs, upd.(DownstreamArguments))
		}
		return nil
	}
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

// Pre: newSize > len(crdt.values)
func (crdt *FloatArrayCrdt) expandArray(newSize int32) {
	newCounts := make([]float64, newSize)
	copy(newCounts, crdt.values)
	crdt.values = newCounts
}

func (crdt *FloatArrayCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	switch typedUpd := downstreamArgs.(type) {
	case FloatArrayIncrement:
		if int(typedUpd.Position) >= len(crdt.values) {
			effectValue = FloatArraySetSizeEffect(len(crdt.values)) //The value of this position before was "0" as it did not belong to the array
			crdt.expandArray(typedUpd.Position + 1)
		} else {
			effectValue = FloatArrayIncrementEffect(typedUpd)
		}
		crdt.values[typedUpd.Position] += typedUpd.Change
	case FloatArrayDecrement:
		if int(typedUpd.Position) >= len(crdt.values) {
			effectValue = FloatArraySetSizeEffect(len(crdt.values))
			crdt.expandArray(typedUpd.Position + 1)
		} else {
			effectValue = FloatArrayDecrementEffect(typedUpd)
		}
		crdt.values[typedUpd.Position] -= typedUpd.Change
	case FloatArrayIncrementAll:
		effectValue = FloatArrayIncrementAllEffect(typedUpd)
		typedChange := float64(typedUpd)
		for i := range crdt.values {
			crdt.values[i] += typedChange
		}
	case FloatArrayDecrementAll:
		effectValue = FloatArrayDecrementAllEffect(typedUpd)
		typedChange := float64(typedUpd)
		for i := range crdt.values {
			crdt.values[i] -= typedChange
		}
	case FloatArrayIncrementMulti:
		if len(typedUpd) > len(crdt.values) {
			effectValue = FloatArrayIncMultiWithSizeEffect{IncEff: FloatArrayIncrementMultiEffect(typedUpd), OldSize: len(crdt.values)}
			crdt.expandArray(int32(len(typedUpd)))
		} else {
			effectValue = FloatArrayIncrementMultiEffect(typedUpd)
		}
		for i, change := range typedUpd {
			crdt.values[i] += change
		}
	case FloatArrayDecrementMulti:
		if len(typedUpd) > len(crdt.values) {
			effectValue = FloatArrayDecMultiWithSizeEffect{DecEff: FloatArrayDecrementMultiEffect(typedUpd), OldSize: len(crdt.values)}
			crdt.expandArray(int32(len(typedUpd)))
		} else {
			effectValue = FloatArrayIncrementMultiEffect(typedUpd)
		}
		for i, change := range typedUpd {
			crdt.values[i] += change
		}

	case FloatArrayIncrementSub:
		oldSize := len(crdt.values)
		if len(typedUpd.Changes) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Positions {
				if int(pos) >= len(crdt.values) {
					crdt.expandArray(pos + 1)
				}
				crdt.values[pos] += change
			}
		} else {
			for i, pos := range typedUpd.Positions {
				if int(pos) >= len(crdt.values) {
					crdt.expandArray(pos + 1)
				}
				if pos == -1 {
					fmt.Printf("[FloatArrayCrdt][ERROR]Position is -1. Changes: %v. Positions: %v\n", typedUpd.Changes, typedUpd.Positions)
				}
				crdt.values[pos] += typedUpd.Changes[i]
			}
		}
		if oldSize != len(crdt.values) {
			effectValue = FloatArrayIncSubWithSizeEffect{IncEff: FloatArrayIncrementSubEffect(typedUpd), OldSize: oldSize}
		} else {
			effectValue = FloatArrayIncrementSubEffect(typedUpd)
		}
	case FloatArrayDecrementSub:
		oldSize := len(crdt.values)
		if len(typedUpd.Changes) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Positions {
				if int(pos) >= len(crdt.values) {
					crdt.expandArray(pos + 1)
				}
				crdt.values[pos] -= change
			}
		} else {
			for i, pos := range typedUpd.Positions {
				if int(pos) >= len(crdt.values) {
					crdt.expandArray(pos + 1)
				}
				crdt.values[pos] -= typedUpd.Changes[i]
			}
		}
		if oldSize != len(crdt.values) {
			effectValue = FloatArrayDecSubWithSizeEffect{DecEff: FloatArrayDecrementSubEffect(typedUpd), OldSize: oldSize}
		} else {
			effectValue = FloatArrayDecrementSubEffect(typedUpd)
		}
	case FloatArrayIncrementRange:
		if int(typedUpd.To) > len(crdt.values) {
			effectValue = FloatArrayIncRangeWithSizeEffect{IncEff: FloatArrayIncrementRangeEffect(typedUpd), OldSize: len(crdt.values)}
			crdt.expandArray(typedUpd.To)
		} else {
			effectValue = FloatArrayIncrementRangeEffect(typedUpd)
		}
		for i := typedUpd.From; i < typedUpd.To; i++ {
			crdt.values[i] += typedUpd.Change
		}
	case FloatArrayDecrementRange:
		if int(typedUpd.To) > len(crdt.values) {
			effectValue = FloatArrayDecRangeWithSizeEffect{DecEff: FloatArrayDecrementRangeEffect(typedUpd), OldSize: len(crdt.values)}
			crdt.expandArray(typedUpd.To)
		} else {
			effectValue = FloatArrayDecrementRangeEffect(typedUpd)
		}
		for i := typedUpd.From; i < typedUpd.To; i++ {
			crdt.values[i] -= typedUpd.Change
		}
	case FloatArraySetSize:
		if int(typedUpd) > len(crdt.values) {
			effectValue = FloatArraySetSizeEffect(len(crdt.values))
			crdt.expandArray(int32(typedUpd))
		} else { //Can still happen (e.g., two concurrent set sizes)
			effectValue = NoEffect{}
		}
	default:
		fmt.Printf("[FloatArray][Downstream]Unsupported downstream type: %v (%T)\n", downstreamArgs, downstreamArgs)
	}
	return &effectValue
}

func (crdt *FloatArrayCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *FloatArrayCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := FloatArrayCrdt{CRDTVM: crdt.CRDTVM.copy(), values: copyToNewFloat64Slice(crdt.values)}
	return &newCRDT
}

func (crdt *FloatArrayCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Most likely can do a small optimization to the one possible for Counters.
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *FloatArrayCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *FloatArrayCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case FloatArrayIncrementEffect:
		crdt.values[typedEffect.Position] -= typedEffect.Change
	case FloatArrayDecrementEffect:
		crdt.values[typedEffect.Position] += typedEffect.Change
	case FloatArrayIncrementAllEffect:
		typedChange := float64(typedEffect)
		for i := range crdt.values {
			crdt.values[i] -= typedChange
		}
	case FloatArrayDecrementAllEffect:
		typedChange := float64(typedEffect)
		for i := range crdt.values {
			crdt.values[i] += typedChange
		}
	case FloatArrayIncrementMultiEffect:
		for i, change := range typedEffect {
			crdt.values[i] -= change
		}
	case FloatArrayDecrementMultiEffect:
		for i, change := range typedEffect {
			crdt.values[i] += change
		}
	case FloatArrayIncrementSubEffect:
		if len(typedEffect.Changes) == 1 {
			change := typedEffect.Changes[0]
			for _, pos := range typedEffect.Positions {
				crdt.values[pos] -= change
			}
		} else {
			for i, pos := range typedEffect.Positions {
				crdt.values[pos] -= typedEffect.Changes[i]
			}
		}
	case FloatArrayDecrementSubEffect:
		if len(typedEffect.Changes) == 1 {
			change := typedEffect.Changes[0]
			for _, pos := range typedEffect.Positions {
				crdt.values[pos] += change
			}
		} else {
			for i, pos := range typedEffect.Positions {
				crdt.values[pos] += typedEffect.Changes[i]
			}
		}
	case FloatArrayIncMultiWithSizeEffect:
		for i, change := range typedEffect.IncEff {
			crdt.values[i] -= change
		}
		crdt.values = crdt.values[:typedEffect.OldSize]
	case FloatArrayDecMultiWithSizeEffect:
		for i, change := range typedEffect.DecEff {
			crdt.values[i] += change
		}
		crdt.values = crdt.values[:typedEffect.OldSize]
	case FloatArrayIncSubWithSizeEffect:
		if len(typedEffect.IncEff.Changes) == 1 {
			change := typedEffect.IncEff.Changes[0]
			for _, pos := range typedEffect.IncEff.Positions {
				crdt.values[pos] -= change
			}
		} else {
			for i, pos := range typedEffect.IncEff.Positions {
				crdt.values[pos] -= typedEffect.IncEff.Changes[i]
			}
		}
		crdt.values = crdt.values[:typedEffect.OldSize]
	case FloatArrayDecSubWithSizeEffect:
		if len(typedEffect.DecEff.Changes) == 1 {
			change := typedEffect.DecEff.Changes[0]
			for _, pos := range typedEffect.DecEff.Positions {
				crdt.values[pos] += change
			}
		} else {
			for i, pos := range typedEffect.DecEff.Positions {
				crdt.values[pos] += typedEffect.DecEff.Changes[i]
			}
		}
		crdt.values = crdt.values[:typedEffect.OldSize]
	case FloatArraySetSizeEffect:
		crdt.values = crdt.values[:typedEffect]
	}
}

func (crdt *FloatArrayCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

func copyToNewFloat64Slice(slice []float64) []float64 {
	newSlice := make([]float64, len(slice))
	copy(newSlice, slice)
	return newSlice
}

func copyToNewFloat64SliceWithSize(slice []float64, sizeOfNewSlice int) []float64 {
	newSlice := make([]float64, sizeOfNewSlice)
	copy(newSlice, slice)
	return newSlice
}

//Protobuf functions

func (crdtOp FloatArrayIncrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetArrayfloatop().GetInc()
	return FloatArrayIncrement{Change: incProto.GetInc(), Position: incProto.GetIndex()}
}

func (crdtOp FloatArrayIncrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_Inc{Inc: &proto.ApbArrayFloatIncrement{Index: pb.Int32(crdtOp.Position), Inc: pb.Float64(crdtOp.Change)}}}}}
}

func (crdtOp FloatArrayDecrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetArrayfloatop().GetInc()
	return FloatArrayIncrement{Change: -incProto.GetInc(), Position: incProto.GetIndex()}
}

func (crdtOp FloatArrayDecrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_Inc{Inc: &proto.ApbArrayFloatIncrement{Index: pb.Int32(crdtOp.Position), Inc: pb.Float64(-crdtOp.Change)}}}}}
}

func (crdtOp FloatArrayIncrementAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return FloatArrayIncrementAll(protobuf.GetArrayfloatop().GetIncAll().GetInc())
}

func (crdtOp FloatArrayIncrementAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_IncAll{IncAll: &proto.ApbArrayFloatIncrementAll{Inc: pb.Float64(float64(crdtOp))}}}}}
}

func (crdtOp FloatArrayDecrementAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return FloatArrayDecrementAll(-protobuf.GetArrayfloatop().GetIncAll().GetInc())
}

func (crdtOp FloatArrayDecrementAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_IncAll{IncAll: &proto.ApbArrayFloatIncrementAll{Inc: pb.Float64(float64(-crdtOp))}}}}}
}

func (crdtOp FloatArrayIncrementMulti) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return FloatArrayIncrementMulti(protobuf.GetArrayfloatop().GetIncMulti().GetIncs())
}

func (crdtOp FloatArrayIncrementMulti) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_IncMulti{IncMulti: &proto.ApbArrayFloatIncrementMulti{Incs: crdtOp}}}}}
}

func (crdtOp FloatArrayDecrementMulti) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	protoIncs := protobuf.GetArrayfloatop().GetIncMulti().GetIncs()
	crdtOp = make([]float64, len(protoIncs))
	for i, inc := range protoIncs {
		crdtOp[i] = -inc
	}
	return crdtOp
}

func (crdtOp FloatArrayDecrementMulti) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	protoIncs := make([]float64, len(crdtOp))
	for i, inc := range crdtOp {
		protoIncs[i] = -inc
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_IncMulti{IncMulti: &proto.ApbArrayFloatIncrementMulti{Incs: protoIncs}}}}}
}

func (crdtOp FloatArrayIncrementSub) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incSubProto := protobuf.GetArrayfloatop().GetIncSub()
	return FloatArrayIncrementSub{Changes: incSubProto.GetIncs(), Positions: incSubProto.GetIndexes()}
}

func (crdtOp FloatArrayIncrementSub) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_IncSub{IncSub: &proto.ApbArrayFloatIncrementSub{Indexes: crdtOp.Positions, Incs: crdtOp.Changes}}}}}
}

func (crdtOp FloatArrayDecrementSub) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incSubProto := protobuf.GetArrayfloatop().GetIncSub()
	protoIncs := incSubProto.GetIncs()
	crdtOp.Changes, crdtOp.Positions = make([]float64, len(protoIncs)), incSubProto.GetIndexes()
	for i, inc := range protoIncs {
		crdtOp.Changes[i] = -inc
	}
	return crdtOp
}

func (crdtOp FloatArrayDecrementSub) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	protoIncs := make([]float64, len(crdtOp.Changes))
	for i, inc := range crdtOp.Changes {
		protoIncs[i] = -inc
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_IncSub{IncSub: &proto.ApbArrayFloatIncrementSub{Indexes: crdtOp.Positions, Incs: protoIncs}}}}}
}

func (crdtOp FloatArrayIncrementRange) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	rangeProto := protobuf.GetArrayfloatop().GetIncRange()
	return FloatArrayIncrementRange{From: rangeProto.GetFrom(), To: rangeProto.GetTo(), Change: rangeProto.GetInc()}
}

func (crdtOp FloatArrayIncrementRange) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_IncRange{IncRange: &proto.ApbArrayFloatIncrementRange{From: pb.Int32(crdtOp.From), To: pb.Int32(crdtOp.To), Inc: pb.Float64(crdtOp.Change)}}}}}
}

func (crdtOp FloatArrayDecrementRange) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	rangeProto := protobuf.GetArrayfloatop().GetIncRange()
	return FloatArrayDecrementRange{From: rangeProto.GetFrom(), To: rangeProto.GetTo(), Change: -rangeProto.GetInc()}
}

func (crdtOp FloatArraySetSize) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return FloatArraySetSize(protobuf.GetArrayfloatop().GetSize().GetSize())
}

func (crdtOp FloatArraySetSize) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &proto.ApbArrayFloatUpdate{
		Upd: &proto.ApbArrayFloatUpdate_Size{Size: &proto.ApbArrayFloatSetSize{Size: pb.Int32(int32(crdtOp))}}}}}
}

func (crdtState FloatArrayCRDTState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return FloatArrayCRDTState(protobuf.GetArrayfloat().GetValues())
}

func (crdtState FloatArrayCRDTState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Arrayfloat{Arrayfloat: &proto.ApbGetArrayFloatResp{Values: crdtState}}}
}

func (crdtState FloatArrayCRDTSingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return FloatArrayCRDTSingleState(protobuf.GetPartread().GetArrayfloat().GetValue())
}

func (crdtState FloatArrayCRDTSingleState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{
		Reply: &proto.ApbPartialReadResp_Arrayfloat{Arrayfloat: &proto.ApbArrayFloatPartialReadResp{Value: pb.Float64(float64(crdtState))}}}}}
}

func (args FloatArraySingleArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args = FloatArraySingleArguments(protobuf.GetArrayfloat().GetSingle().GetIndex())
	return args
}

func (args FloatArraySingleArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Arrayfloat{Arrayfloat: &proto.ApbArrayFloatPartialRead{
		Single: &proto.ApbArrayFloatSingleRead{Index: pb.Int32(int32(args))}}}}
}

func (args FloatArraySubArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args = FloatArraySubArguments(protobuf.GetArrayfloat().GetSub().GetIndexes())
	return args
}

func (args FloatArraySubArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Arrayfloat{Arrayfloat: &proto.ApbArrayFloatPartialRead{
		Sub: &proto.ApbArrayFloatSubRead{Indexes: args}}}}
}

func (args FloatArrayRangeArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	rangeProto := protobuf.GetArrayfloat().GetRange()
	return FloatArrayRangeArguments{From: rangeProto.GetFrom(), To: rangeProto.GetTo()}
}

func (args FloatArrayRangeArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Arrayfloat{Arrayfloat: &proto.ApbArrayFloatPartialRead{
		Range: &proto.ApbArrayFloatRangeRead{From: &args.From, To: &args.To}}}}
}

func (args FloatArrayExceptArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args = FloatArrayExceptArguments(protobuf.GetArrayfloat().GetExcept().GetIndex())
	return args
}

func (args FloatArrayExceptArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Arrayfloat{Arrayfloat: &proto.ApbArrayFloatPartialRead{
		Except: &proto.ApbArrayFloatExceptRead{Index: pb.Int32(int32(args))}}}}
}

func (args FloatArrayExceptRangeArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	rangeProto := protobuf.GetArrayfloat().GetExceptRange()
	return FloatArrayExceptRangeArguments{ExceptRange: rangeProto.GetIndexes(), NPositionsSkip: rangeProto.GetNPositionsSkip()}
}

func (args FloatArrayExceptRangeArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Arrayfloat{Arrayfloat: &proto.ApbArrayFloatPartialRead{
		ExceptRange: &proto.ApbArrayFloatExceptRangeRead{Indexes: args.ExceptRange, NPositionsSkip: pb.Int32(args.NPositionsSkip)}}}}
}

func (downOp FloatArrayIncrement) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	incProto := protobuf.GetArrayFloatOp().GetInc()
	return FloatArrayIncrement{Change: incProto.GetInc(), Position: incProto.GetIndex()}
}

func (downOp FloatArrayIncrement) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.TRUE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_Inc{Inc: &proto.ProtoArrayFloatIncrementDownstream{Index: pb.Int32(downOp.Position), Inc: pb.Float64(downOp.Change)}}}}}
}

func (downOp FloatArrayDecrement) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	incProto := protobuf.GetArrayFloatOp().GetInc()
	return FloatArrayIncrement{Change: -incProto.GetInc(), Position: incProto.GetIndex()}
}

func (downOp FloatArrayDecrement) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.FALSE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_Inc{Inc: &proto.ProtoArrayFloatIncrementDownstream{Index: pb.Int32(downOp.Position), Inc: pb.Float64(-downOp.Change)}}}}}
}

func (downOp FloatArrayIncrementAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return FloatArrayIncrementAll(protobuf.GetArrayFloatOp().GetIncAll().GetInc())
}

func (downOp FloatArrayIncrementAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.TRUE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_IncAll{IncAll: &proto.ProtoArrayFloatIncrementAllDownstream{Inc: pb.Float64(float64(downOp))}}}}}
}

func (downOp FloatArrayDecrementAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return FloatArrayDecrementAll(protobuf.GetArrayFloatOp().GetIncAll().GetInc())
}

func (downOp FloatArrayDecrementAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.FALSE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_IncAll{IncAll: &proto.ProtoArrayFloatIncrementAllDownstream{Inc: pb.Float64(float64(-downOp))}}}}}
}

func (downOp FloatArrayIncrementMulti) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return FloatArrayIncrementMulti(protobuf.GetArrayFloatOp().GetIncMulti().GetIncs())
}

func (downOp FloatArrayIncrementMulti) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.TRUE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_IncMulti{IncMulti: &proto.ProtoArrayFloatIncrementMultiDownstream{Incs: downOp}}}}}
}

func (downOp FloatArrayDecrementMulti) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	protoIncs := protobuf.GetArrayFloatOp().GetIncMulti().GetIncs()
	downOp = make([]float64, len(protoIncs))
	for i, inc := range protoIncs {
		downOp[i] = -inc
	}
	return downOp
}

func (downOp FloatArrayDecrementMulti) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoIncs := make([]float64, len(downOp))
	for i, inc := range downOp {
		protoIncs[i] = -inc
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.FALSE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_IncMulti{IncMulti: &proto.ProtoArrayFloatIncrementMultiDownstream{Incs: protoIncs}}}}}
}

func (downOp FloatArrayIncrementSub) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	incSubProto := protobuf.GetArrayFloatOp().GetIncSub()
	return FloatArrayIncrementSub{Changes: incSubProto.GetIncs(), Positions: incSubProto.GetIndexes()}
}

func (downOp FloatArrayIncrementSub) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.TRUE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_IncSub{IncSub: &proto.ProtoArrayFloatIncrementSubDownstream{Indexes: downOp.Positions, Incs: downOp.Changes}}}}}
}

func (downOp FloatArrayDecrementSub) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	incSubProto := protobuf.GetArrayFloatOp().GetIncSub()
	protoIncs := incSubProto.GetIncs()
	downOp.Changes, downOp.Positions = make([]float64, len(protoIncs)), incSubProto.GetIndexes()
	for i, inc := range protoIncs {
		downOp.Changes[i] = -inc
	}
	return downOp
}

func (downOp FloatArrayDecrementSub) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoIncs := make([]float64, len(downOp.Changes))
	for i, inc := range downOp.Changes {
		protoIncs[i] = -inc
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.FALSE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_IncSub{IncSub: &proto.ProtoArrayFloatIncrementSubDownstream{Indexes: downOp.Positions, Incs: protoIncs}}}}}
}

func (downOp FloatArrayIncrementRange) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	rangeProto := protobuf.GetArrayFloatOp().GetIncRange()
	return FloatArrayIncrementRange{From: rangeProto.GetFrom(), To: rangeProto.GetTo(), Change: rangeProto.GetInc()}
}

func (downOp FloatArrayIncrementRange) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.TRUE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_IncRange{IncRange: &proto.ProtoArrayFloatIncrementRangeDownstream{From: pb.Int32(downOp.From), To: pb.Int32(downOp.To), Inc: pb.Float64(downOp.Change)}}}}}
}

func (downOp FloatArrayDecrementRange) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	rangeProto := protobuf.GetArrayFloatOp().GetIncRange()
	return FloatArrayDecrementRange{From: rangeProto.GetFrom(), To: rangeProto.GetTo(), Change: -rangeProto.GetInc()}
}

func (downOp FloatArrayDecrementRange) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{
		IsInc: shared.FALSE_POINTER, Upd: &proto.ProtoArrayFloatDownstream_IncRange{IncRange: &proto.ProtoArrayFloatIncrementRangeDownstream{From: pb.Int32(downOp.From), To: pb.Int32(downOp.To), Inc: pb.Float64(-downOp.Change)}}}}}
}

func (downOp FloatArraySetSize) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return FloatArraySetSize(protobuf.GetArrayFloatOp().GetSize().GetSize())
}

func (downOp FloatArraySetSize) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ArrayFloatOp{ArrayFloatOp: &proto.ProtoArrayFloatDownstream{Upd: &proto.ProtoArrayFloatDownstream_Size{Size: &proto.ProtoArraySetSize{Size: pb.Int32(int32(downOp))}}}}}
}

func (crdt *FloatArrayCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{State: &proto.ProtoState_ArrayFloat{ArrayFloat: &proto.ProtoArrayFloatState{Values: crdt.values}}}
}

func (crdt *FloatArrayCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID uint16) (newCRDT CRDT) {
	return (&FloatArrayCrdt{values: proto.GetArrayFloat().GetValues()}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *FloatArrayCrdt) GetCRDT() CRDT { return crdt }
