package antidote

import (
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
	"potionDB/src/shared"
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
	snaps     map[clocksi.TimestampKey]crdt.CRDT
	replicaID int16
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

func (vm *SnapshotVM) ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp, updsNotYetApplied []*crdt.UpdateArguments) (state crdt.State) {
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

func (vm *SnapshotVM) ReadLatest(readArgs crdt.ReadArguments, updsNotYetApplied []*crdt.UpdateArguments) (state crdt.State) {
	return vm.Read(readArgs, updsNotYetApplied)
}

func (vm *SnapshotVM) Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.DownstreamArguments) (otherDownstreamArgs crdt.DownstreamArguments) {
	if shared.IsVMDisabled {
		return vm.CRDT.Downstream(updTs, downstreamArgs)
	}
	otherDownstreamArgs = vm.CRDT.Downstream(updTs, downstreamArgs)
	vm.snaps[updTs.GetMapKey()] = vm.CRDT.(crdt.InversibleCRDT).Copy()
	return
}

func (vm *SnapshotVM) GetLatestCRDT() (crdt crdt.CRDT) {
	return vm.CRDT
}
