package crdt

import "potionDB/shared/shared"

// Used by CounterArrayCrdt.
var int64SlicePool = shared.MakeNewSlicePool[[]int64]()
var int32SlicePool = shared.MakeNewSlicePool[[]int32]()     //Many protobufs since slices of int32 are used for more memory efficient passing of IDs/values.
var float64SlicePool = shared.MakeNewSlicePool[[]float64]() //Currently unused.

var bytesSlicePool = shared.MakeNewSlicePool[[][]byte]()

// This one maybe we could make a slice of pools (ugh), and then initialize with atomic (CAS?) when we initialize the CRDT.
// The CAS will add overhead, as we'll be forced to store this as a unsafe.Pointer.
// We'll just initialize all of them at the start, it's OK.
//var counterMapPoolInt64 *shared.SliceSinglePool[[]KeyCounterPair[int64], KeyCounterPair[int64]]
//var counterMapPoolInt32 *shared.SliceSinglePool[[]KeyCounterPair[int32], KeyCounterPair[int32]]
//var counterMapPoolInt16 *shared.SliceSinglePool[[]KeyCounterPair[int16], KeyCounterPair[int16]]
//var counterMapPoolInt8 *shared.SliceSinglePool[[]KeyCounterPair[int8], KeyCounterPair[int8]]
//var counterMapPoolInt *shared.SliceSinglePool[[]KeyCounterPair[int], KeyCounterPair[int]]
//var counterMapPoolFloat64 *shared.SliceSinglePool[[]KeyCounterPair[float64], KeyCounterPair[float64]]
//var counterMapPoolFloat32 *shared.SliceSinglePool[[]KeyCounterPair[float32], KeyCounterPair[float32]]

const (
	COUNTER_MAP_POOL_MIN_LEN = 500
)

var counterMapPoolInt64 = shared.MakeNewSliceSinglePool[[]KeyCounterPair[int64]](COUNTER_MAP_POOL_MIN_LEN)
var counterMapPoolInt32 = shared.MakeNewSliceSinglePool[[]KeyCounterPair[int32]](COUNTER_MAP_POOL_MIN_LEN)
var counterMapPoolInt16 = shared.MakeNewSliceSinglePool[[]KeyCounterPair[int16]](COUNTER_MAP_POOL_MIN_LEN)
var counterMapPoolInt8 = shared.MakeNewSliceSinglePool[[]KeyCounterPair[int8]](COUNTER_MAP_POOL_MIN_LEN)
var counterMapPoolInt = shared.MakeNewSliceSinglePool[[]KeyCounterPair[int]](COUNTER_MAP_POOL_MIN_LEN)
var counterMapPoolFloat64 = shared.MakeNewSliceSinglePool[[]KeyCounterPair[float64]](COUNTER_MAP_POOL_MIN_LEN)
var counterMapPoolFloat32 = shared.MakeNewSliceSinglePool[[]KeyCounterPair[float32]](COUNTER_MAP_POOL_MIN_LEN)

//Most others seem to be OK. Later need to test protobufing to see if it needs pools too (and if any other CRDTs now show up in the memory profile.)
//TODO: Use the pools.

//And do proto3.
