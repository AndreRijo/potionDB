package crdt

import "potionDB/src/clocksi"

//Implements the CRDTVM interface for ease of integration with CRDTs
type SnapshopCrdt struct{}

func (crdt *SnapshopCrdt) copy() (copyCRDT CRDTVM) {
	return crdt
}

func (crdt *SnapshopCrdt) rebuildCRDTToVersion(targetTs clocksi.Timestamp) {

}

func (crdt *SnapshopCrdt) addToHistory(ts *clocksi.Timestamp, updArgs *DownstreamArguments, effect *Effect) {

}
