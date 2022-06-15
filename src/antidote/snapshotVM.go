package antidote

import (
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
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
	snaps map[clocksi.TimestampKey]crdt.CRDT
}

func (vm SnapshotVM) Initialize(newCrdt crdt.CRDT, currTs clocksi.Timestamp) (newVm SnapshotVM) {
	newVm = SnapshotVM{
		CRDT:  newCrdt,
		snaps: make(map[clocksi.TimestampKey]crdt.CRDT),
	}
	//newVm.snaps[currTs.GetMapKey()] = newCrdt.Copy()
	return
}

func (vm *SnapshotVM) Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.DownstreamArguments) (otherDownstreamArgs crdt.DownstreamArguments) {
	return vm.CRDT.Downstream(updTs, downstreamArgs)
}

type SnapshopCrdt struct {
	vm *SnapshotVM
}
