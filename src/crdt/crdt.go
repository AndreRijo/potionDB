package crdt

type CRDT interface {
	Initialize() (newCrdt CRDT)

	GetValue() (state State)

	Update(args UpdateArguments) (downstreamArgs UpdateArguments)

	Downstream(downstreamArgs UpdateArguments)
}

type State interface {
}

//Represents the arguments specific to each CRDT.
type UpdateArguments interface {
}

//TODO: Seems like we can't implement methods with more specific structures (e.g: use CounterState when interface mention State).
//TODO: This means that we'll need some kind of typechecking, as the counter CRDT should only deal with counter states/arguments.
