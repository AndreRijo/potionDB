package antidote

import (
	"clocksi"
	"crdt"
)

type VersionManager interface {
	ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp) (state crdt.State)

	ReadLatest(readArgs crdt.ReadArguments) (state crdt.State)

	Update(updArgs crdt.UpdateArguments) crdt.UpdateArguments

	Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.UpdateArguments)
}
