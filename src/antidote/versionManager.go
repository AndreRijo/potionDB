package antidote

import (
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
)

//The public interface for the materializer
type VersionManager interface {
	ReadOld(readArgs crdt.ReadArguments, readTs clocksi.Timestamp, updsNotYetApplied []*crdt.UpdateArguments) (state crdt.State)

	ReadLatest(readArgs crdt.ReadArguments, updsNotYetApplied []*crdt.UpdateArguments) (state crdt.State)

	Update(updArgs crdt.UpdateArguments) crdt.DownstreamArguments

	Downstream(updTs clocksi.Timestamp, downstreamArgs crdt.DownstreamArguments) (otherDownstreamArgs crdt.DownstreamArguments)

	GetLatestCRDT() (crdt crdt.CRDT)
}

//The interface for each CRDT to interact with its version management metadata
type CRDTVM interface {
	copy()                                                                                      //Copies the internal metadata
	rebuildCRDTToVersion(targetTs clocksi.Timestamp)                                            //Uses the metadata to rebuild to the requested version
	addToHistory(ts *clocksi.Timestamp, updArgs *crdt.DownstreamArguments, effect *crdt.Effect) //Registers an update of the CRDT.
}
