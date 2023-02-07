package antidote

import (
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
)

//Implements VersionManager interface defined in versionManager.go
//The main idea here is to rebuild old versions by applying the inverse operations on a more recent version

type InverseOpVM struct {
	crdt.CRDT                                      //Note that CRDT is embedded, so we can access its methods directly
	OldVersions map[clocksi.TimestampKey]crdt.CRDT //Contains the previously calculated old versions of the CRDT
	//KeysToTimestamp map[clocksi.TimestampKey]clocksi.Timestamp //Allows to obtain the timestamp from its map key representation. Useful because we can't use Timestamp as key...
	updatedSinceGC bool              //Keeps track if this CRDT needs to be GC
	currVersion    clocksi.Timestamp //Version of crdt.CRDT
}

//PUBLIC METHODS

func (vm *InverseOpVM) Initialize(newCrdt crdt.CRDT, currTs clocksi.Timestamp) (newVm VersionManager) {
	newVm = &InverseOpVM{
		CRDT:        newCrdt,
		OldVersions: make(map[clocksi.TimestampKey]crdt.CRDT),
		//KeysToTimestamp: make(map[clocksi.TimestampKey]clocksi.Timestamp),
	}
	return
}

func (vm *InverseOpVM) ReadLatest(readArgs crdt.ReadArguments, updsNotYetApplied []*crdt.UpdateArguments) (state crdt.State) {
	return vm.Read(readArgs, updsNotYetApplied)
}

func (vm *InverseOpVM) ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp, updsNotYetApplied []*crdt.UpdateArguments) (state crdt.State) {
	readTsKey := readTs.GetMapKey()
	cachedCRDT, hasCached := vm.OldVersions[readTsKey]
	//Requested version isn't in cache, so we need to reconstruct the CRDT
	if !hasCached {
		oldCrdt := vm.getClosestMatch(readTs)
		cachedCRDT = vm.reverseOperations(oldCrdt, readTs)
		vm.OldVersions[readTsKey] = cachedCRDT
		//vm.KeysToTimestamp[readTsKey] = readTs
	}
	return cachedCRDT.Read(readArgs, updsNotYetApplied)
}

func (vm *InverseOpVM) GetLatestCRDT() (crdt crdt.CRDT) {
	return vm.CRDT
}

func (vm *InverseOpVM) Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.DownstreamArguments) (otherDownstreamArgs crdt.DownstreamArguments) {
	vm.updatedSinceGC, vm.currVersion = true, updTs
	return vm.CRDT.Downstream(updTs, downstreamArgs)
}

func (vm *InverseOpVM) GC(safeClk clocksi.Timestamp, safeClkKey clocksi.TimestampKey) {
	if !vm.updatedSinceGC {
		return
	}
	if safeClk.IsHigher(vm.currVersion) { //If an object has not been updated in a while, it may happen the latest version is < safeClk
		//Can clean everything
		vm.OldVersions = make(map[clocksi.TimestampKey]crdt.CRDT)
		vm.updatedSinceGC = false //Cleared all past versions so can set this to false.
	} else {
		//This algorithm keeps any key > safeClk and the highestKey that is <= safeClk (highestSafe)
		var highestSafe clocksi.TimestampKey = nil
		for cacheKey := range vm.OldVersions {
			if cacheKey.IsLowerOrEqual(safeClkKey) {
				if highestSafe == nil {
					highestSafe = cacheKey
				} else if cacheKey.IsHigher(highestSafe) {
					//Can delete safely the previous key
					delete(vm.OldVersions, highestSafe)
					highestSafe = cacheKey
				}
			}
		}
		//Do not set updatedSinceGC to false as there is still past versions to be cleaned by a future GC.
		//Do not delete highestSafe - This contains the version that is correct at the time of safeClk
	}
	vm.CRDT.(crdt.CRDTVM).GC(safeClk)
}

//PRIVATE METHODS

func (vm *InverseOpVM) getClosestMatch(readTs clocksi.Timestamp) (crdt crdt.CRDT) {
	var smallestClkTs clocksi.TimestampKey = nil
	readTsKey := readTs.GetMapKey()
	for cacheKey, cacheCRDT := range vm.OldVersions {
		if cacheKey.IsHigher(readTsKey) && (smallestClkTs == nil || cacheKey.IsLower(smallestClkTs)) {
			smallestClkTs, crdt = cacheKey, cacheCRDT
		}
	}
	/*
		for cacheClkKey, cacheClk := range vm.KeysToTimestamp {
			if cacheClk.IsHigher(readTs) && (smallestClk == nil || cacheClk.IsLower(smallestClk)) {
				smallestClk, crdt = cacheClk, vm.OldVersions[cacheClkKey]
			}
		}
	*/
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
