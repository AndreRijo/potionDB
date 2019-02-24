package antidote

import (
	"clocksi"
	"crdt"
)

//Implements VersionManager interface defined in versionManager.go
//The main idea here is to rebuild old versions by applying the inverse operations on a more recent version

type InverseOpVM struct {
	crdt.CRDT                                   //Note that CRDT is embedded, so we can access its methods directly
	OldVersions map[clocksi.Timestamp]crdt.CRDT //TODO: We can't use Timestamp here! I need to fix this somehow...
}

//PUBLIC METHODS

func (vm *InverseOpVM) Initialize(newCrdt crdt.CRDT) (newVm InverseOpVM) {
	newVm = InverseOpVM{
		CRDT:        newCrdt,
		OldVersions: make(map[clocksi.Timestamp]crdt.CRDT),
	}
	return
}

func (vm *InverseOpVM) ReadLatest(readArgs crdt.ReadArguments) (state crdt.State) {
	return vm.Read(readArgs)
}

func (vm *InverseOpVM) ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp) (state crdt.State) {
	cachedCRDT, hasCached := vm.OldVersions[readTs]
	//Requested version isn't in cache, so we need to reconstruct the CRDT
	if !hasCached {
		oldCrdt := vm.getClosestMatch(readTs)
		cachedCRDT = vm.reverseOperations(oldCrdt, readTs)
		vm.OldVersions[readTs] = cachedCRDT
	}
	return cachedCRDT.Read(readArgs)
}

//PRIVATE METHODS

func (vm *InverseOpVM) getClosestMatch(readTs clocksi.Timestamp) (crdt crdt.CRDT) {
	var smallestClk clocksi.Timestamp = clocksi.DummyHighTs

	for cacheClk, cacheCrdt := range vm.OldVersions {
		if cacheClk.IsHigher(readTs) && cacheClk.IsLower(smallestClk) {
			smallestClk, crdt = cacheClk, cacheCrdt
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
