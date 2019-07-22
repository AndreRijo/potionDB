package crdt

import (
	"clocksi"
	rand "math/rand"
	"time"
)

/*
	Grow-only map, based on the GSet.
	Add conflicts are solved using timestamps
	TODO: Finish
	Problem: Most likelly needs a ts per entry
*/

type GMapCrdt struct {
	*genericInversibleCRDT
	values         map[string]Element
	ts             int64
	replicaID      int64
	localReplicaID int64 //ReplicaID of the replica which has this CRDT instance
}

type DownstreamGMapAddAll struct {
	Values    map[string]Element
	Ts        int64
	ReplicaID int64
}

//Effect for inversibleCRDT

type GMapAddAllEffect struct {
	ReplacedValues  map[string]Element  //Entries for which there was a previous value (and stores that value)
	NotExistentKeys map[string]struct{} //Entries which didn't exist before
	Ts              int64               //Ts before this effect
	ReplicaID       int64               //ReplicaID of the Ts before this effect
}

func (args DownstreamGMapAddAll) MustReplicate() bool { return true }

//Note: crdt can (and most often will be) nil
func (crdt *GMapCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int64) (newCrdt CRDT) {
	crdt = &GMapCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		values:                make(map[string]Element),
		ts:                    0,
		replicaID:             replicaID,
		localReplicaID:        replicaID,
	}
	newCrdt = crdt
	return
}

func (crdt *GMapCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArg := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case GetValueArguments:
		return crdt.getValue(typedArg, updsNotYetApplied)
	case HasKeyArguments:
		return crdt.hasKey(typedArg, updsNotYetApplied)
	case GetKeysArguments:
		return crdt.getKeys(updsNotYetApplied)
	}
	return nil
}

func (crdt *GMapCrdt) getState(updsNotYetApplied []UpdateArguments) (state MapEntryState) {
	copyMap := make(map[string]Element)
	//Copy the existing values to the new map
	for key, value := range crdt.values {
		copyMap[key] = value
	}

	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		//Return right away
		return MapEntryState{Values: copyMap}
	}

	//Go through all adds in updsNotYetApplied and, for each key, keep the most recent one
	//For each key, the latest add is always the latest upd in updsNotYetApplied that refers to that key
	for _, upd := range updsNotYetApplied {
		updAdds := upd.(DownstreamGMapAddAll)
		for key, value := range updAdds.Values {
			copyMap[key] = value
		}
	}

	return MapEntryState{Values: copyMap}
}

func (crdt *GMapCrdt) getKeys(updsNotYetApplied []UpdateArguments) (state MapKeysState) {
	//Literally the same as getState, but only storing the keys. Yay, code repetition!
	//It's more efficient than calling getState and then removing the values though...

	i := 0

	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		keys := make([]string, len(crdt.values))
		//Copy the existing values to the array and return right away
		for key := range crdt.values {
			keys[i] = key
			i++
		}
		return MapKeysState{Keys: keys}
	}

	keys := make([]string, len(crdt.values)+len(updsNotYetApplied))
	//Copy the existing values to the array and return right away
	for key := range crdt.values {
		keys[i] = key
		i++
	}

	for _, upd := range updsNotYetApplied {
		updAdds := upd.(DownstreamGMapAddAll)
		for key := range updAdds.Values {
			if _, has := crdt.values[key]; !has {
				keys[i] = key
				i++
			}
		}
	}

	return MapKeysState{Keys: keys}
}

func (crdt *GMapCrdt) getValue(args GetValueArguments, updsNotYetApplied []UpdateArguments) (state MapGetValueState) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		//Return right away
		return MapGetValueState{Value: crdt.values[args.Key]}
	}
	value := crdt.values[args.Key]
	//Search if there is a more recent value
	for _, upd := range updsNotYetApplied {
		updAdds := upd.(DownstreamGMapAddAll)
		if newValue, newHas := updAdds.Values[args.Key]; newHas {
			value = newValue
		}
	}

	return MapGetValueState{Value: value}
}

func (crdt *GMapCrdt) hasKey(args HasKeyArguments, updsNotYetApplied []UpdateArguments) (state MapHasKeyState) {
	_, hasKey := crdt.values[args.Key]
	if hasKey || updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		//Return right away. Remember that this CRDT doesn't support removes, so once an add is found there's no need to search further.
		return MapHasKeyState{HasKey: hasKey}
	}

	//Search if it was added in updsNotYetApplied
	for _, upd := range updsNotYetApplied {
		updAdds := upd.(DownstreamGMapAddAll)
		if _, hasKey = updAdds.Values[args.Key]; hasKey {
			//Found it, no need to look further
			return MapHasKeyState{HasKey: true}
		}
	}
	//Didn't find it
	return MapHasKeyState{HasKey: false}
}

//TODO: Maybe one day implement add with its own method (i.e., avoid the overhead of creating/handling maps?)
func (crdt *GMapCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch typedArgs := args.(type) {
	case MapAdd:
		argMap := make(map[string]Element)
		argMap[typedArgs.Key] = typedArgs.Value
		downstreamArgs = crdt.getAddAllDownstreamArgs(argMap)
	case MapAddAll:
		downstreamArgs = crdt.getAddAllDownstreamArgs(typedArgs.Values)
	}
	return
}

func (crdt *GMapCrdt) getAddAllDownstreamArgs(values map[string]Element) (downstreamArgs DownstreamGMapAddAll) {
	ts := time.Now().UTC().UnixNano()
	if ts < crdt.ts {
		ts = crdt.ts + rand.Int63n(100)
	}
	return DownstreamGMapAddAll{Values: values, Ts: ts, ReplicaID: crdt.localReplicaID}
}

func (crdt *GMapCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *GMapCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	/*
		switch opType := downstreamArgs.(type) {
		case DownstreamGMapAddAll:
			effect = crdt.applyAddAll(opType.Values)
		case *DownstreamGMapAddAll:
			effect = crdt.applyAddAll(opType.Values)
		}
	*/
	//fmt.Println("[GMapCrdt]State after downstream: ", crdt.getState(nil))
	return
}

/*
func (crdt *GMapCrdt) applyAddAll(toAdd map[Element]Unique) (effect *Effect) {
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

*/
func (crdt *GMapCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	//TODO: Typechecking
	return true, nil
}

//METHODS FOR INVERSIBLE_CRDT

func (crdt *GMapCrdt) Copy() (copyCRDT InversibleCRDT) {
	/*
		newCrdt := GMapCrdt{
			genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
			elems:                 make(map[Element]UniqueSet),
			random:                rand.NewSource(time.Now().Unix()),
		}
		for element, uniques := range crdt.elems {
			newCrdt.elems[element] = uniques.copy()
		}

		return &newCrdt
	*/
	return crdt
}

func (crdt *GMapCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp)
}

func (crdt *GMapCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *GMapCrdt) undoEffect(effect *Effect) {
	/*
		switch typedEffect := (*effect).(type) {
		case AddAllEffect:
			crdt.undoAddAllEffect(&typedEffect)
		case RemoveAllEffect:
			crdt.undoRemoveAllEffect(&typedEffect)
		}
	*/
}

/*
func (crdt *GMapCrdt) undoAddAllEffect(effect *AddAllEffect) {
	for key, uniqueToRem := range effect.AddedMap {
		if existingUniques, ok := crdt.elems[key]; ok {
			delete(existingUniques, uniqueToRem)
			if len(existingUniques) == 0 {
				delete(crdt.elems, key)
			}
		}
	}
}
*/
