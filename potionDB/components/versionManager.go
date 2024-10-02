package components

import (
	fmt "fmt"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
)

// The public interface for the materializer
type VersionManager interface {
	Initialize(newCrdt crdt.CRDT, currTs clocksi.Timestamp) (newVm VersionManager)

	ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp, updsNotYetApplied []crdt.UpdateArguments) (state crdt.State)

	ReadLatest(readArgs crdt.ReadArguments, updsNotYetApplied []crdt.UpdateArguments) (state crdt.State)

	Update(updArgs crdt.UpdateArguments) crdt.DownstreamArguments

	Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.DownstreamArguments) (otherDownstreamArgs crdt.DownstreamArguments)

	GetLatestCRDT() (crdt crdt.CRDT)

	GC(safeClk clocksi.Timestamp, safeClkKey clocksi.TimestampKey)
}

type VMType byte

const (
	InversibleVMType VMType = 1
	SnapshotVMType   VMType = 2
)

var (
	VmTypeToUse VMType = InversibleVMType //Configuration variable
	BaseVM      VersionManager
)

func SetVMToUse() {
	switch VmTypeToUse {
	case InversibleVMType:
		BaseVM = &InverseOpVM{}
	case SnapshotVMType:
		BaseVM = &SnapshotVM{}
	default:
		fmt.Println("[VM]Warning - Unknown VMType", VmTypeToUse, ". Will use Inversible.")
		BaseVM = &InverseOpVM{}
	}
}
