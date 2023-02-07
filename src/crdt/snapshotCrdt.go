package crdt

import "potionDB/src/clocksi"

//Implements the CRDTVM interface for ease of integration with CRDTs
type SnapshotCrdt struct{}

func (crdt *SnapshotCrdt) copy() (copyCRDT CRDTVM) {
	return crdt
}

func (crdt *SnapshotCrdt) rebuildCRDTToVersion(targetTs clocksi.Timestamp) {

}

func (crdt *SnapshotCrdt) addToHistory(ts *clocksi.Timestamp, updArgs *DownstreamArguments, effect *Effect) {

}

func (crdt *SnapshotCrdt) GC(safeClk clocksi.Timestamp) {

}
