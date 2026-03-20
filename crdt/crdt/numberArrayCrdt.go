package crdt

//Leaving this here as an example of why we can't have generic CRDTs :)
//We can't define the same method twice for two different types.
//This means that protobuf functions would have to be aware of all instanceable types... having to do switches... inneficient.
//Better have separate CRDTs for each type. Possibly I could still define this one day if saving on memory usage/data transfer is more relevant
//Than the extra code overhead (e.g., an array of int8s)

/*package crdt

import "potionDB/crdt/proto"

type NumberArrayCrdt[T Number] struct {
	CRDTVM
	entries []T
}

//States

type NumberArrayState[T Number] []T

type NumberArraySingleState[T Number] struct {
	Value T
}

func (args NumberArraySingleState[AnyInt]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_INT
}

func (args NumberArraySingleState[AnyFloat]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_FLOAT
}

func (args NumberArraySingleState[AnyUint]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_UINT
}

//Reads

// Position
type NumberArraySingleArguments[T Number] int32
type NumberArrayExceptArguments[T Number] int32

// Positions
type NumberArraySubArguments[T Number] []int32

type NumberArrayExceptRangeArguments[T Number] struct {
	ExceptRange    []int32 //Ranges to skip. Evens: start. Odds: end.
	NPositionsSkip int32   //Optional. This helps to create a read slice with a more appropriate dimension.
}

// Updates
type NumberArraySetSize int32

type NumberArrayIncrement[T Number] struct {
	Change   T
	Position int32
}

type NumberArrayDecrement[T Number] struct {
	Change   T
	Position int32
}

// If len(Changes) == 1, then increments all positions in Positions by Changes[0]
// Otherwise, assumes len(changes) == len(positions)
type NumberArrayIncrementSub[T Number] struct {
	Changes   []T
	Positions []int32
}

type NumberArrayDecrementSub[T Number] struct {
	Changes   []T
	Positions []int32
}

type NumberArrayIncrementAll[T Number] struct {
	Value T
}
type NumberArrayDecrementAll[T Number] struct {
	Value T
}

// []T: Changes.
type NumberArrayIncrementMulti[T Number] []T
type NumberArrayDecrementMulti[T Number] []T

type NumberArrayIncrementEffect[T Number] NumberArrayIncrement[T]
type NumberArrayDecrementEffect[T Number] NumberArrayDecrement[T]
type NumberArrayIncrementSubEffect[T Number] NumberArrayIncrementSub[T]
type NumberArrayDecrementSubEffect[T Number] NumberArrayDecrementSub[T]
type NumberArrayIncrementAllEffect[T Number] NumberArrayIncrementAll[T]
type NumberArrayDecrementAllEffect[T Number] NumberArrayDecrementAll[T]
type NumberArrayIncrementMultiEffect[T Number] NumberArrayIncrementMulti[T]
type NumberArrayDecrementMultiEffect[T Number] NumberArrayDecrementMulti[T]
type NumberArraySetSizeEffect[T Number] NumberArraySetSize
type NumberArrayIncSubWithSizeEffect[T Number] struct {
	IncEff  NumberArrayIncrementSubEffect[T]
	OldSize int
}
type NumberArrayDecSubWithSizeEffect[T Number] struct {
	DecEff  NumberArrayDecrementSubEffect[T]
	OldSize int
}
type NumberArrayIncMultiWithSizeEffect[T Number] struct {
	IncEff  NumberArrayIncrementMultiEffect[T]
	OldSize int
}
type NumberArrayDecMultiWithSizeEffect[T Number] struct {
	DecEff  NumberArrayDecrementMultiEffect[T]
	OldSize int
}

func (crdt *NumberArrayCrdt) GetCRDTType() proto.CRDTType {}
*/
