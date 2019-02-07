package crdt

import "clocksi"

type CRDT interface {
	Initialize() (newCrdt CRDT)

	GetValue() (state State)

	Update(args UpdateArguments) (downstreamArgs UpdateArguments)

	Downstream(downstreamArgs UpdateArguments)

	GetVersion() (ts clocksi.Timestamp)

	IsOperationWellTyped(args UpdateArguments) (ok bool, err error)
}

//The idea is to include here the methods/data common to every CRDT. For now, that's only the vectorClock and GetVersion()
type genericCRDT struct {
	ts clocksi.Timestamp
}

type State interface {
}

//Represents the arguments specific to each CRDT.
type UpdateArguments interface {
}

type ArgsError struct {
	err  string
	args UpdateArguments
}

//TODO: Seems like we can't implement methods with more specific structures (e.g: use CounterState when interface mention State).
//TODO: This means that we'll need some kind of typechecking, as the counter CRDT should only deal with counter states/arguments.

func (crdt genericCRDT) GetVersion() (ts clocksi.Timestamp) {
	ts = crdt.ts
	return
}
