//A CRDT that stores nothing and accepts any type of operation, ignoring it.
//Useful for certain kinds of debugging and analyzing memory usage.

package crdt

import "clocksi"

type EmptyCrdt struct{}

type EmptyState struct{}

func (args EmptyState) GetCRDTType() CRDTType { return CRDTType_LWWREG }

func (crdt *EmptyCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int64) (newCrdt CRDT) {
	return crdt
}

func (crdt *EmptyCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	return EmptyState{}
}

func (crdt *EmptyCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	return NoOp{}
}

func (crdt *EmptyCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	return nil
}

func (crdt *EmptyCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *EmptyCrdt) Copy() (copyCRDT InversibleCRDT) { return nil }

func (crdt *EmptyCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {}

func (crdt *EmptyCrdt) undoEffect(effect *Effect) {}

func (crdt *EmptyCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) { return nil }

func (crdt *EmptyCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}
