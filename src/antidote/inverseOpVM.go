package antidote

import (
	"clocksi"
	"crdt"
)

//Implements VersionManager interface defined in versionManager.go
//The main idea here is to rebuild old versions by applying the inverse operations on a more recent version

type InverseOpVM struct {
	crdt.CRDT                                                  //Note that CRDT is embedded, so we can access its methods directly
	OldVersions     map[clocksi.TimestampKey]crdt.CRDT         //Contains the previously calculated old versions of the CRDT
	KeysToTimestamp map[clocksi.TimestampKey]clocksi.Timestamp //Allows to obtain the timestamp from its map key representation. Useful because we can't use Timestamp as key...
}

//PUBLIC METHODS

func (vm *InverseOpVM) Initialize(newCrdt crdt.CRDT) (newVm InverseOpVM) {
	newVm = InverseOpVM{
		CRDT:            newCrdt,
		OldVersions:     make(map[clocksi.TimestampKey]crdt.CRDT),
		KeysToTimestamp: make(map[clocksi.TimestampKey]clocksi.Timestamp),
	}
	return
}

func (vm *InverseOpVM) ReadLatest(readArgs crdt.ReadArguments, updsNotYetApplied []crdt.UpdateArguments) (state crdt.State) {
	return vm.Read(readArgs, updsNotYetApplied)
}

func (vm *InverseOpVM) ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp, updsNotYetApplied []crdt.UpdateArguments) (state crdt.State) {
	readTsKey := readTs.GetMapKey()
	cachedCRDT, hasCached := vm.OldVersions[readTsKey]
	//Requested version isn't in cache, so we need to reconstruct the CRDT
	if !hasCached {
		oldCrdt := vm.getClosestMatch(readTs)
		cachedCRDT = vm.reverseOperations(oldCrdt, readTs)
		vm.OldVersions[readTsKey] = cachedCRDT
	}
	return cachedCRDT.Read(readArgs, updsNotYetApplied)
}

//PRIVATE METHODS

func (vm *InverseOpVM) getClosestMatch(readTs clocksi.Timestamp) (crdt crdt.CRDT) {
	var smallestClk clocksi.Timestamp = nil

	for cacheClkKey, cacheClk := range vm.KeysToTimestamp {
		if cacheClk.IsHigher(readTs) && (smallestClk == nil || cacheClk.IsLower(smallestClk)) {
			smallestClk, crdt = cacheClk, vm.OldVersions[cacheClkKey]
		}
	}
	//Either there's no version cached or all cached are too old, so we'll need to use the latest version
	if crdt == nil {
		crdt = vm.CRDT
	}
	return
}

func (vm *InverseOpVM) reverseOperations(oldCrdt crdt.CRDT, readTs clocksi.Timestamp) (copyCrdt crdt.InversibleCRDT) {
	copyCrdt = oldCrdt.(crdt.InversibleCRDT).Copy()
	copyCrdt.RebuildCRDTToVersion(readTs)
	return
}
