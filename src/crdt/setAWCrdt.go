package crdt

import (
	"clocksi"
	rand "math/rand"
	"time"
)

type Element string

//Note: Implements both CRDT and InversibleCRDT
type SetAWCrdt struct {
	genericInversibleCRDT
	elems map[Element]uniqueSet
	//elems map[Element]uniqueSet
	//Used to generate unique identifiers. This does not need to be included in a serialization to transfer the state.
	random rand.Source
}

type SetAWValueState struct {
	Elems []Element
}

type SetAWLookupState struct {
	hasElem bool
}

type LookupReadArguments struct {
	elem Element
}

//Operations

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

//Downstream operations

type DownstreamAddAll struct {
	elems map[Element]unique
}

type DownstreamRemoveAll struct {
	elems map[Element]uniqueSet
}

//Operation effects for inversibleCRDT (the first two are probably not needed)

type AddEffect struct {
	Elem   Element
	Unique unique
}

type RemoveEffect struct {
	Elem    Element
	Uniques uniqueSet
}

type AddAllEffect struct {
	AddedMap map[Element]unique
}

type RemoveAllEffect struct {
	RemovedMap map[Element]uniqueSet
}

//Note: crdt can (and most often will be) nil
func (crdt *SetAWCrdt) Initialize() (newCrdt CRDT) {
	crdt = &SetAWCrdt{
		genericInversibleCRDT: genericInversibleCRDT{}.initialize(),
		elems:                 make(map[Element]uniqueSet),
		random:                rand.NewSource(time.Now().Unix())} //TODO: Assign to crdt is potencially unecessary (idea: Updates self in the map (reset operation?))
	newCrdt = crdt
	return
}

//TODO: Implement proper read
/*
func (crdt *SetAWCrdt) Read(args ReadArguments) (state State) {
	return crdt.GetValue()
}
*/

//TODO: Implement proper read
func (crdt *SetAWCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return crdt.GetValue()
	}

	adds := make(map[Element]struct{})
	rems := make(map[Element]struct{})

	//Idea: go through the updates and check which elements were added/removed compared to the original state.
	//Note that we shouldn't use the state returned by GetValue() - that state is an array of elements,
	//which would be very inneficient for checking which elements to add/remove (not to mention expand the array capacity)

	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case Add:
			if crdt.elems[typedUpd.Element] == nil {
				adds[typedUpd.Element] = struct{}{}
			} else {
				delete(rems, typedUpd.Element)
			}
		case AddAll:
			for _, elem := range typedUpd.Elems {
				if crdt.elems[elem] == nil {
					adds[elem] = struct{}{}
				} else {
					delete(rems, elem)
				}
			}
		case Remove:
			if crdt.elems[typedUpd.Element] != nil {
				rems[typedUpd.Element] = struct{}{}
			} else {
				delete(adds, typedUpd.Element)
			}
		case RemoveAll:
			for _, elem := range typedUpd.Elems {
				if crdt.elems[elem] != nil {
					rems[elem] = struct{}{}
				} else {
					delete(adds, elem)
				}
			}
		}
	}

	elems := make([]Element, len(crdt.elems)+len(adds)-len(rems))
	i := 0
	for elem := range crdt.elems {
		if _, hasRem := rems[elem]; !hasRem {
			elems[i] = elem
			i++
		}
	}
	for elem := range adds {
		elems[i] = elem
		i++
	}
	return SetAWValueState{Elems: elems}
}

func (crdt *SetAWCrdt) GetValue() (state State) {
	//go doesn't have a set structure nor a way to get keys from map.
	//Using an auxiliary array in the state with the elements isn't a good option either - remove would have to search for the element
	//So, unfortunatelly, we need to built it here.
	elems := make([]Element, len(crdt.elems))
	i := 0
	for key := range crdt.elems {
		elems[i] = key
		i++
	}
	state = SetAWValueState{Elems: elems}
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

func (crdt *SetAWCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs UpdateArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)
}

func (crdt *SetAWCrdt) applyDownstream(downstreamArgs UpdateArguments) (effect *Effect) {
	switch opType := downstreamArgs.(type) {
	case DownstreamAddAll:
		effect = crdt.applyAddAll(opType.elems)
	case DownstreamRemoveAll:
		effect = crdt.applyRemoveAll(opType.elems)
	}
	return
}

func (crdt *SetAWCrdt) applyAddAll(toAdd map[Element]unique) (effect *Effect) {
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
	var effectValue Effect = AddAllEffect{AddedMap: toAdd}
	return &effectValue
}

func (crdt *SetAWCrdt) applyRemoveAll(toRem map[Element]uniqueSet) (effect *Effect) {
	removedMap := make(map[Element]uniqueSet)
	var effectValue Effect = RemoveAllEffect{RemovedMap: removedMap}

	for key, uniquesToRem := range toRem {
		//Checks if the key is already in the map. If it is, removes the uniques in the intersection
		if existingUniques, ok := crdt.elems[key]; ok {
			removedMap[key] = existingUniques.getAndRemoveIntersection(uniquesToRem)
			if len(existingUniques) == 0 {
				delete(crdt.elems, key)
			}
		}
		//The element wasn't in the set already, so nothing to do
	}
	return &effectValue
}

func (crdt *SetAWCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	//TODO: Typechecking
	return true, nil
}

//METHODS FOR INVERSIBLE_CRDT

func (crdt *SetAWCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := SetAWCrdt{
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		elems:                 make(map[Element]uniqueSet),
		random:                rand.NewSource(time.Now().Unix()),
	}
	for element, uniques := range crdt.elems {
		newCrdt.elems[element] = uniques.copy()
	}

	return &newCrdt
}

func (crdt *SetAWCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp)
}

func (crdt *SetAWCrdt) reapplyOp(updArgs UpdateArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *SetAWCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case AddAllEffect:
		crdt.undoAddAllEffect(&typedEffect)
	case RemoveAllEffect:
		crdt.undoRemoveAllEffect(&typedEffect)
	}
}

func (crdt *SetAWCrdt) undoAddAllEffect(effect *AddAllEffect) {
	for key, uniqueToRem := range effect.AddedMap {
		if existingUniques, ok := crdt.elems[key]; ok {
			delete(existingUniques, uniqueToRem)
			if len(existingUniques) == 0 {
				delete(crdt.elems, key)
			}
		}
	}
}

func (crdt *SetAWCrdt) undoRemoveAllEffect(effect *RemoveAllEffect) {
	for key, uniquesToAdd := range effect.RemovedMap {
		if existingUniques, ok := crdt.elems[key]; ok {
			existingUniques.addAll(uniquesToAdd)
		} else {
			newSet := makeUniqueSet()
			newSet.addAll(uniquesToAdd)
			crdt.elems[key] = newSet
		}
	}
}
