package antidote

import (
	"clocksi"
	"crdt"
)

type VersionManager interface {
	ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp, updsNotYetApplied []crdt.UpdateArguments) (state crdt.State)

	ReadLatest(readArgs crdt.ReadArguments, updsNotYetApplied []crdt.UpdateArguments) (state crdt.State)

	Update(updArgs crdt.UpdateArguments) crdt.UpdateArguments

	Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.UpdateArguments)
}