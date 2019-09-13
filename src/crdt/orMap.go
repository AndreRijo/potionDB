package crdt

import (
	"clocksi"
	rand "math/rand"
	"time"
)

const CRDTType_ORMAP CRDTType = 15

type ORMapCrdt struct {
	*genericInversibleCRDT
	entries map[string]map[Element]UniqueSet
	//Used to generate unique identifiers. This should not be included in a serialization to transfer the state.
	random rand.Source
}

//States returned by queries

type MapEntryState struct {
	Values map[string]Element
}

type MapGetValueState struct {
	Value Element
}

type MapHasKeyState struct {
	HasKey bool
}

type MapKeysState struct {
	Keys []string
}

//Queries

type GetValueArguments struct {
	Key string
}

type HasKeyArguments struct {
	Key string
}

type GetKeysArguments struct{}

//Operations

type MapAdd struct {
	Key   string
	Value Element
}

type MapRemove struct {
	Key string
}

type MapAddAll struct {
	Values map[string]Element
}

type MapRemoveAll struct {
	Keys []string
}

//Downstream operations

type DownstreamORMapAddAll struct {
	Adds map[string]UniqueElemPair
	Rems map[string]map[Element]UniqueSet //Remove previous entries for the keys we're adding
}

type DownstreamORMapRemoveAll struct {
	Rems map[string]map[Element]UniqueSet
}

//Operation effects for inversibleCRDT

type ORMapAddAllEffect struct {
	Adds map[string]UniqueElemPair
	Rems map[string]map[Element]UniqueSet
}

type ORMapRemoveAllEffect struct {
	Rems map[string]map[Element]UniqueSet
}

func (args MapAdd) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args MapRemove) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args MapAddAll) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args MapRemoveAll) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args DownstreamORMapAddAll) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args DownstreamORMapRemoveAll) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args MapEntryState) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args MapGetValueState) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args MapHasKeyState) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args MapKeysState) GetCRDTType() CRDTType { return CRDTType_ORMAP }

func (args DownstreamORMapAddAll) MustReplicate() bool { return true }

func (args DownstreamORMapRemoveAll) MustReplicate() bool { return true }

//Note: crdt can (and most often will be) nil
func (crdt *ORMapCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int64) (newCrdt CRDT) {
	crdt = &ORMapCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		entries:               make(map[string]map[Element]UniqueSet),
		random:                rand.NewSource(time.Now().Unix())} //TODO: Assign to crdt is potencially unecessary (idea: Updates self in the map (reset operation?))
	newCrdt = crdt
	return
}

func (crdt *ORMapCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArg := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case GetKeysArguments:
		return crdt.getKeys(updsNotYetApplied)
	case GetValueArguments:
		return crdt.getValue(updsNotYetApplied, typedArg.Key)
	case HasKeyArguments:
		return crdt.hasKey(updsNotYetApplied, typedArg.Key)
	}
	return nil
}

func (crdt *ORMapCrdt) getState(updsNotYetApplied []UpdateArguments) (state MapEntryState) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		values := make(map[string]Element)
		for key, elemMap := range crdt.entries {
			values[key] = crdt.getMinElem(elemMap)
		}
		return MapEntryState{Values: values}
	}

	adds := make(map[string]Element)
	rems := make(map[string]struct{})

	//Key idea: for each key, the latest update is the only one that matters, hence start at the end.
	for i := len(updsNotYetApplied) - 1; i >= 0; i-- {
		switch typedUpd := updsNotYetApplied[i].(type) {
		case MapAdd:
			if _, has := rems[typedUpd.Key]; !has {
				adds[typedUpd.Key] = typedUpd.Value
			}
		case MapRemove:
			if _, has := adds[typedUpd.Key]; !has {
				rems[typedUpd.Key] = struct{}{}
			}
		case MapAddAll:
			for key, elem := range typedUpd.Values {
				if _, has := rems[key]; !has {
					adds[key] = elem
				}
			}

		case MapRemoveAll:
			for _, key := range typedUpd.Keys {
				if _, has := adds[key]; !has {
					rems[key] = struct{}{}
				}
			}
		}
	}

	//Build state
	values := make(map[string]Element)
	for key, elemMap := range crdt.entries {
		if _, has := rems[key]; !has {
			values[key] = crdt.getMinElem(elemMap)
		}
	}
	for key, elem := range adds {
		values[key] = elem
	}
	return MapEntryState{Values: values}
}

func (crdt *ORMapCrdt) getKeys(updsNotYetApplied []UpdateArguments) (state MapKeysState) {
	//Basically the same as getState, but only storing the keys. Yay, code repetition!
	//It's more efficient than calling getState and then removing the values though...

	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		keys := make([]string, len(crdt.entries))
		i := 0
		for key, _ := range crdt.entries {
			keys[i] = key
			i++
		}
		return MapKeysState{Keys: keys}
	}

	adds := make(map[string]struct{})
	rems := make(map[string]struct{})

	//Key idea: for each key, the latest update is the only one that matters, hence start at the end.
	for i := len(updsNotYetApplied) - 1; i >= 0; i-- {
		switch typedUpd := updsNotYetApplied[i].(type) {
		case MapAdd:
			if _, has := rems[typedUpd.Key]; !has {
				adds[typedUpd.Key] = struct{}{}
			}
		case MapRemove:
			if _, has := adds[typedUpd.Key]; !has {
				rems[typedUpd.Key] = struct{}{}
			}
		case MapAddAll:
			for key, _ := range typedUpd.Values {
				if _, has := rems[key]; !has {
					adds[key] = struct{}{}
				}
			}

		case MapRemoveAll:
			for _, key := range typedUpd.Keys {
				if _, has := adds[key]; !has {
					rems[key] = struct{}{}
				}
			}
		}
	}

	//Build state
	keys := make([]string, len(crdt.entries)+len(adds)-len(rems))
	i := 0
	for key, _ := range crdt.entries {
		if _, has := rems[key]; !has {
			keys[i] = key
			i++
		}
	}
	for key := range adds {
		if _, has := crdt.entries[key]; !has {
			keys[i] = key
			i++
		}
	}
	return MapKeysState{Keys: keys}
}

func (crdt *ORMapCrdt) getValue(updsNotYetApplied []UpdateArguments, key string) (state MapGetValueState) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		//Return right away
		return MapGetValueState{Value: crdt.getMinElem(crdt.entries[key])}
	}
	//Idea: search for the element from the end - the last local upd for a given key is always the one that decides
	for i := len(updsNotYetApplied) - 1; i >= 0; i-- {
		switch typedUpd := updsNotYetApplied[i].(type) {
		case MapAdd:
			if typedUpd.Key == key {
				return MapGetValueState{Value: typedUpd.Value}
			}
		case MapRemove:
			if typedUpd.Key == key {
				return MapGetValueState{}
			}

		case MapAddAll:
			if e, has := typedUpd.Values[key]; has {
				return MapGetValueState{Value: e}
			}
		case MapRemoveAll:
			for _, k := range typedUpd.Keys {
				if k == key {
					return MapGetValueState{}
				}
			}
		}
	}
	return MapGetValueState{Value: crdt.getMinElem(crdt.entries[key])}
}

func (crdt *ORMapCrdt) hasKey(updsNotYetApplied []UpdateArguments, key string) (state MapHasKeyState) {
	value := crdt.getValue(updsNotYetApplied, key)
	if (value != MapGetValueState{}) {
		return MapHasKeyState{HasKey: true}
	}
	return MapHasKeyState{HasKey: false}
}

//TODO: Maybe one day implement add and remove with their own methods (i.e., avoid the overhead of creating/handling arrays and maps?)
func (crdt *ORMapCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch opType := args.(type) {
	case MapAdd:
		values := make(map[string]Element)
		values[opType.Key] = opType.Value
		downstreamArgs = crdt.getAddAllDownstreamArgs(values)
	case MapRemove:
		keys := make([]string, 1)
		keys[0] = opType.Key
		downstreamArgs = crdt.getRemoveAllDownstreamArgs(keys)
	case MapAddAll:
		downstreamArgs = crdt.getAddAllDownstreamArgs(opType.Values)
	case MapRemoveAll:
		downstreamArgs = crdt.getRemoveAllDownstreamArgs(opType.Keys)
	}
	return
}

func (crdt *ORMapCrdt) getAddAllDownstreamArgs(values map[string]Element) (downstreamArgs DownstreamORMapAddAll) {
	addsUniques := make(map[string]UniqueElemPair)
	remsUniques := make(map[string]map[Element]UniqueSet)
	for key, elem := range values {
		addsUniques[key] = UniqueElemPair{Element: elem, Unique: Unique(crdt.random.Int63())}
		if entry, has := crdt.entries[key]; has {
			remMap := make(map[Element]UniqueSet)
			for elem, uniques := range entry {
				remMap[elem] = uniques.copy()
			}
			remsUniques[key] = remMap
		}
	}
	return DownstreamORMapAddAll{addsUniques, remsUniques}
}

func (crdt *ORMapCrdt) getRemoveAllDownstreamArgs(keys []string) (downstreamArgs DownstreamORMapRemoveAll) {
	uniques := make(map[string]map[Element]UniqueSet)
	for _, key := range keys {
		keyUniques := make(map[Element]UniqueSet)
		for elem, remUniques := range crdt.entries[key] {
			keyUniques[elem] = remUniques.copy()
		}
		uniques[key] = keyUniques
	}
	return DownstreamORMapRemoveAll{uniques}
}

func (crdt *ORMapCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *ORMapCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var tmpEffect Effect = NoEffect{}
	switch opType := downstreamArgs.(type) {
	case DownstreamORMapAddAll:
		tmpEffect = crdt.applyAddAll(opType.Adds, opType.Rems)
	case DownstreamORMapRemoveAll:
		tmpEffect = crdt.applyRemoveAll(opType.Rems)
	}
	return &tmpEffect
}

func (crdt *ORMapCrdt) applyAddAll(toAdd map[string]UniqueElemPair, toRem map[string]map[Element]UniqueSet) (effect *ORMapAddAllEffect) {
	//Remove old entries for the keys we're adding
	remEffect := crdt.applyRemoveAll(toRem)
	//For each key, add the respective element
	for key, pair := range toAdd {
		crdtEntry := crdt.entries[key]
		if crdtEntry == nil {
			crdtEntry = make(map[Element]UniqueSet)
			crdtEntry[pair.Element] = makeUniqueSet()
			crdtEntry[pair.Element].add(pair.Unique)
			crdt.entries[key] = crdtEntry
		} else {
			crdtUniques := crdtEntry[pair.Element]
			if crdtUniques == nil {
				crdtUniques = makeUniqueSet()
				crdtEntry[pair.Element] = crdtUniques
			}
			crdtUniques.add(pair.Unique)
		}
	}

	return &ORMapAddAllEffect{toAdd, remEffect.Rems}
}

func (crdt *ORMapCrdt) applyRemoveAll(toRem map[string]map[Element]UniqueSet) (effect *ORMapRemoveAllEffect) {
	removed := make(map[string]map[Element]UniqueSet)
	for key, entry := range toRem {
		if crdtEntry, has := crdt.entries[key]; has {
			removedElems := make(map[Element]UniqueSet)
			//For this key, remove uniques for each element
			for elem, uniques := range entry {
				if crdtUniques, has := crdtEntry[elem]; has {
					removedElems[elem] = crdtUniques.getAndRemoveIntersection(uniques)
					if len(crdtUniques) == 0 {
						delete(crdtEntry, elem)
					}
				}
			}
			if len(removedElems) > 0 {
				removed[key] = removedElems
			}
			if len(crdtEntry) == 0 {
				delete(crdt.entries, key)
			}
		}
	}
	return &ORMapRemoveAllEffect{toRem}
}

func (crdt *ORMapCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	//TODO: Typechecking
	return true, nil
}

func (crdt *ORMapCrdt) getMinElem(elemMap map[Element]UniqueSet) (minElem Element) {
	for elem := range elemMap {
		if minElem == "" || elem < minElem {
			minElem = elem
		}
	}
	return
}

//METHODS FOR INVERSIBLE_CRDT

func (crdt *ORMapCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := ORMapCrdt{
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		entries:               make(map[string]map[Element]UniqueSet),
		random:                rand.NewSource(time.Now().Unix()),
	}
	for key, elemMap := range crdt.entries {
		newElemMap := make(map[Element]UniqueSet)
		for elem, uniques := range elemMap {
			newElemMap[elem] = uniques.copy()
		}
		newCrdt.entries[key] = newElemMap
	}

	return &newCrdt
}

func (crdt *ORMapCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
}

func (crdt *ORMapCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *ORMapCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case ORMapAddAllEffect:
		crdt.undoAddAllEffect(typedEffect.Adds, typedEffect.Rems)
	case ORMapRemoveAllEffect:
		crdt.undoRemoveAllEffect(typedEffect.Rems)
	}
}

func (crdt *ORMapCrdt) undoAddAllEffect(adds map[string]UniqueElemPair, rems map[string]map[Element]UniqueSet) {
	//Remove all entries (uniques) in addMap;
	//call undoRemoveAllEffect in removeMap
	for key, elemToRem := range adds {
		elemsEntry := crdt.entries[key]
		uniquesEntry := elemsEntry[elemToRem.Element]
		delete(uniquesEntry, elemToRem.Unique)
		if len(uniquesEntry) == 0 {
			delete(elemsEntry, elemToRem.Element)
			if len(elemsEntry) == 0 {
				delete(crdt.entries, key)
			}
		}
	}
	crdt.undoRemoveAllEffect(rems)
}

func (crdt *ORMapCrdt) undoRemoveAllEffect(rems map[string]map[Element]UniqueSet) {
	//Add back all entries (uniques) in remMap
	for key, elemsToAdd := range rems {
		keyEntry, has := crdt.entries[key]
		if !has {
			keyEntry = make(map[Element]UniqueSet)
			crdt.entries[key] = keyEntry
		}
		for elem, uniques := range elemsToAdd {
			if elemEntry, has := keyEntry[elem]; has {
				elemEntry.addAll(uniques)
			} else {
				elemEntry = makeUniqueSet()
				elemEntry.addAll(uniques)
				keyEntry[elem] = elemEntry
			}
		}
	}
}

func (crdt *ORMapCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}
