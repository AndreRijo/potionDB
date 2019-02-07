package crdt

import (
	rand "math/rand"
	"time"
)

type Element string

type SetAWCrdt struct {
	genericCRDT
	elems map[Element]uniqueSet
	//elems map[Element]uniqueSet
	//Used to generate unique identifiers. This does not need to be included in a serialization to transfer the state.
	random rand.Source
}

type SetAWState struct {
	Elems []Element
}

type Add struct {
	Element Element
}

type Remove struct {
	Element Element
}

type AddAll struct {
	Elems []Element
}

type RemoveAll struct {
	Elems []Element
}

type DownstreamAddAll struct {
	elems map[Element]unique
}

type DownstreamRemoveAll struct {
	elems map[Element]uniqueSet
}

//Note: crdt can (and most often will be) nil
func (crdt *SetAWCrdt) Initialize() (newCrdt CRDT) {
	crdt = &SetAWCrdt{elems: make(map[Element]uniqueSet),
		random: rand.NewSource(time.Now().Unix())} //TODO: Assign to crdt is potencially unecessary (idea: Updates self in the map (reset operation?))
	newCrdt = crdt
	return
}

func (crdt *SetAWCrdt) GetValue() (state State) {
	//go doesn't have a set structure nor a way to get keys from map.
	//Using an auxiliary array in the state with the elements isn't a good option either - remove would have to search for the element
	//So, unfortunatelly, we need to built it here.
	elems := make([]Element, len(crdt.elems))
	i := 0
	//TODO: Check if "key" actually has keys or values
	for key := range crdt.elems {
		elems[i] = key
		i++
	}
	state = SetAWState{Elems: elems}
	return
}

//TODO: Maybe one day implement add and remove with their own methods (i.e., avoid the overhead of creating/handling arrays and maps?)
func (crdt *SetAWCrdt) Update(args UpdateArguments) (downstreamArgs UpdateArguments) {
	switch opType := args.(type) {
	case Add:
		elemArray := make([]Element, 1)
		elemArray[0] = opType.Element
		downstreamArgs = crdt.getAddAllDownstreamArgs(elemArray)
	case Remove:
		elemArray := make([]Element, 1)
		elemArray[0] = opType.Element
		downstreamArgs = crdt.getRemoveAllDownstreamArgs(elemArray)
	case AddAll:
		downstreamArgs = crdt.getAddAllDownstreamArgs(opType.Elems)
	case RemoveAll:
		downstreamArgs = crdt.getRemoveAllDownstreamArgs(opType.Elems)
	}
	return
}

func (crdt *SetAWCrdt) getAddAllDownstreamArgs(elems []Element) (downstreamArgs UpdateArguments) {
	uniqueMap := make(map[Element]unique)
	for _, key := range elems {
		uniqueMap[key] = unique(crdt.random.Int63())
	}
	downstreamArgs = DownstreamAddAll{elems: uniqueMap}
	return
}

func (crdt *SetAWCrdt) getRemoveAllDownstreamArgs(elems []Element) (downstreamArgs UpdateArguments) {
	uniqueMap := make(map[Element]uniqueSet)
	for _, key := range elems {
		//In go, the "=" symbol does a deep copy. This is true for any object, unless you use references
		uniqueMap[key] = crdt.elems[key]
	}
	downstreamArgs = DownstreamRemoveAll{elems: uniqueMap}
	return
}

func (crdt *SetAWCrdt) Downstream(downstreamArgs UpdateArguments) {
	switch opType := downstreamArgs.(type) {
	case DownstreamAddAll:
		crdt.applyAddAll(opType.elems)
	case DownstreamRemoveAll:
		crdt.applyRemoveAll(opType.elems)
	}
}

func (crdt *SetAWCrdt) applyAddAll(toAdd map[Element]unique) {
	for key, newUnique := range toAdd {
		//Checks if the key is already in the map. If it is, adds a unique
		if existingUniques, ok := crdt.elems[key]; ok {
			existingUniques.add(newUnique)
		} else {
			newSet := makeUniqueSet()
			newSet.add(newUnique)
			crdt.elems[key] = newSet
		}
	}
}

func (crdt *SetAWCrdt) applyRemoveAll(toRem map[Element]uniqueSet) {
	for key, uniquesToRem := range toRem {
		//Checks if the key is already in the map. If it is, removes the uniques in the intersection
		if existingUniques, ok := crdt.elems[key]; ok {
			existingUniques.removeAllIn(uniquesToRem)
			if len(existingUniques) == 0 {
				delete(crdt.elems, key)
			}
		}
		//The element wasn't in the set already, so nothing to do
	}
}
