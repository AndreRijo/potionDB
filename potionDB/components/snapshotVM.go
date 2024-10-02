package components

import (
	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/shared/shared"
)

/*type VersionManager interface {
	ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp, updsNotYetApplied []*crdt.UpdateArguments) (state crdt.State)

	ReadLatest(readArgs crdt.ReadArguments, updsNotYetApplied []*crdt.UpdateArguments) (state crdt.State)

	Update(updArgs crdt.UpdateArguments) crdt.DownstreamArguments

	Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.DownstreamArguments) (otherDownstreamArgs crdt.DownstreamArguments)

	GetLatestCRDT() (crdt crdt.CRDT)
}
*/

//Implements VersionManager interface defined in versionManager.go
//This VM keeps snapshots (copies) of each version of the CRDT.

type SnapshotVM struct {
	crdt.CRDT
	snaps          map[clocksi.TimestampKey]crdt.CRDT
	replicaID      int16
	updatedSinceGC bool              //Keeps track if this CRDT needs to be GC
	currVersion    clocksi.Timestamp //Version of crdt.CRDT
}

func (vm *SnapshotVM) Initialize(newCrdt crdt.CRDT, currTs clocksi.Timestamp) (newVm VersionManager) {
	if shared.IsVMDisabled {
		return &SnapshotVM{CRDT: newCrdt}
	}
	newVmS := &SnapshotVM{
		CRDT:      newCrdt,
		snaps:     make(map[clocksi.TimestampKey]crdt.CRDT),
		replicaID: shared.ReplicaID,
	}
	newVmS.snaps[currTs.GetMapKey()] = newCrdt.(crdt.InversibleCRDT).Copy()
	return newVmS
}

func (vm *SnapshotVM) ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp, updsNotYetApplied []crdt.UpdateArguments) (state crdt.State) {
	if shared.IsVMDisabled {
		return vm.ReadLatest(readArgs, updsNotYetApplied)
	}
	oldCRDT, has := vm.snaps[readTs.GetMapKey()]
	if has {
		return oldCRDT.Read(readArgs, updsNotYetApplied)
	}
	//CRDT didn't exist at that time, we'll create an empty one.
	oldCRDT = crdt.InitializeCrdt(vm.CRDT.GetCRDTType(), vm.replicaID)
	vm.snaps[readTs.GetMapKey()] = oldCRDT
	return oldCRDT.Read(readArgs, updsNotYetApplied)
}

func (vm *SnapshotVM) ReadLatest(readArgs crdt.ReadArguments, updsNotYetApplied []crdt.UpdateArguments) (state crdt.State) {
	return vm.Read(readArgs, updsNotYetApplied)
}

func (vm *SnapshotVM) Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.DownstreamArguments) (otherDownstreamArgs crdt.DownstreamArguments) {
	if shared.IsVMDisabled {
		return vm.CRDT.Downstream(updTs, downstreamArgs)
	}
	vm.updatedSinceGC, vm.currVersion = true, updTs
	otherDownstreamArgs = vm.CRDT.Downstream(updTs, downstreamArgs)
	vm.snaps[updTs.GetMapKey()] = vm.CRDT.(crdt.InversibleCRDT).Copy()
	return
}

func (vm *SnapshotVM) GetLatestCRDT() (crdt crdt.CRDT) {
	return vm.CRDT
}

func (vm *SnapshotVM) GC(safeClk clocksi.Timestamp, safeClkKey clocksi.TimestampKey) {
	//Same algorithm as in inverseOpVM.
	if !vm.updatedSinceGC {
		return
	}
	if safeClk.IsHigher(vm.currVersion) { //If an object has not been updated in a while, it may happen the latest version is < safeClk
		//Can clean everything
		vm.snaps = make(map[clocksi.TimestampKey]crdt.CRDT)
		vm.updatedSinceGC = false //Cleared all past versions so can set this to false.
	} else {
		//This algorithm keeps any key > safeClk and the highestKey that is <= safeClk (highestSafe)
		var highestSafe clocksi.TimestampKey = nil
		for cacheKey := range vm.snaps {
			if cacheKey.IsLowerOrEqual(safeClkKey) {
				if highestSafe == nil {
					highestSafe = cacheKey
				} else if cacheKey.IsHigher(highestSafe) {
					//Can delete safely the previous key
					delete(vm.snaps, highestSafe)
					highestSafe = cacheKey
				}
			}
		}
		//Do not set updatedSinceGC to false as there is still past versions to be cleaned by a future GC.
		//Do not delete highestSafe - This contains the version that is correct at the time of safeClk
	}
	vm.CRDT.(crdt.CRDTVM).GC(safeClk)
}
