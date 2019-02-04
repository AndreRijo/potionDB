package crdt

type CounterCrdt struct {
	Value int32
}

type CounterState struct {
	Value int32
}

type Increment struct {
	Change int32
}

type Decrement struct {
	Change int32
}

//Note: crdt can (and most often will be) nil
func (crdt *CounterCrdt) Initialize() (newCrdt CRDT) {
	crdt = &CounterCrdt{Value: 0} //TODO: Assign to crdt is potencially unecessary (idea: Updates self in the map (reset operation?))
	newCrdt = crdt
	return
}

func (crdt *CounterCrdt) GetValue() (state State) {
	state = CounterState{Value: crdt.Value}
	return
}

func (crdt *CounterCrdt) Update(args UpdateArguments) (downstreamArgs UpdateArguments) {
	downstreamArgs = args
	return
}

func (crdt *CounterCrdt) Downstream(downstreamArgs UpdateArguments) {
	switch incOrDec := downstreamArgs.(type) {
	case Increment:
		crdt.Value += incOrDec.Change
	case Decrement:
		crdt.Value -= incOrDec.Change
	}
}
