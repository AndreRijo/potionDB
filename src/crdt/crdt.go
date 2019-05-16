package crdt

import "clocksi"

type CRDT interface {
	Initialize(startTs *clocksi.Timestamp) (newCrdt CRDT)

	Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State)

	Update(args UpdateArguments) (downstreamArgs UpdateArguments)

	Downstream(updTs clocksi.Timestamp, downstreamArgs UpdateArguments)

	//GetVersion() (ts clocksi.Timestamp) //TODO: This is probably not needed at all

	IsOperationWellTyped(args UpdateArguments) (ok bool, err error)

	//Returns an instance of each possible downstream argument that this CRDT might generate
	//This is needed for serialization, as it needs to know which types will be used per interface.
	//The internal values of each returned type is irrelevant, as long as all types of downstream arguments are covered
	GetPossibleDownstreamTypes() (possibleTypes []UpdateArguments)
}

//TODO: Whenever a new CRDT is added, add a dummy instance here. This is needed for remoteConnection.go. This might no longer be needed
var (
	DummyCRDTs = []CRDT{&SetAWCrdt{}, &CounterCrdt{}}
)

//The idea is to include here the methods/data common to every CRDT. For now, there's... nothing
type genericCRDT struct {
}

type State interface {
}

//Represents the update arguments specific to each CRDT.
type UpdateArguments interface {
}

//Represents the read arguments specific to each CRDT.
type ReadArguments interface {
}

//Represents a read of the whole state
type StateReadArguments struct {
}

type ArgsError struct {
	err  string
	args UpdateArguments
}

func (crdt genericCRDT) initialize() (newCrdt genericCRDT) {
	return genericCRDT{}
}

//Note that this only copies the generic part
func (crdt genericCRDT) copy() (copyCrdt genericCRDT) {
	return genericCRDT{}
}
