package crdt

import "potionDB/src/clocksi"

//The interface for each CRDT to interact with its version management metadata
type CRDTVM interface {
	copy() (copyCrdt CRDTVM)                                                          //Copies the internal metadata
	rebuildCRDTToVersion(targetTs clocksi.Timestamp)                                  //Uses the metadata to rebuild to the requested version
	addToHistory(ts *clocksi.Timestamp, updArgs *DownstreamArguments, effect *Effect) //Registers an update of the CRDT.
}
