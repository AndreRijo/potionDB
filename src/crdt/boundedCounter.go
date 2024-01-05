package crdt

import (
	"fmt"
	"potionDB/src/clocksi"
	"potionDB/src/proto"

	pb "github.com/golang/protobuf/proto"
)

/*Have a periodic task that asks rights from other replicas
Algorithm:
Check if local rights < ((total rights / nReplicas)/2
If yes, continue and search for replica with highest rights
Ask for (remote rights - local rights) / 2 rights	(note: consider capping the value at total rights / nReplicas)
Wait for reply
On the other replica, execute if the rights to give are not over half
Commit and Reply; Send to replication
Receive reply, apply transfer locally, commit.
Note: The local replica must ignore this operation/txn when it is received from replication.

Question: Should this "transaction" be separate from the normal transactions or go through the materializer normally?
Another option would be to ask for a transfer after using rights and it going below the threshold.
But if doing this case, better to ensure the transfer is done in the background.

Note: Everyone needs to execute the transfer operation so that the permissions map stays up-to-date.

To avoid concurrency on counter access... two solutions:
TM/Other entity does a transaction involving all bounded counters in every partition to do a transfer request;
Each Materializer partition queues into itself a transaction for all bounded counters of that partition.
Better 2nd solution. That way it won't slow down the system when multiple partitions are being updated.
To think: should each CRDT generate a message or group them?
*/

//Supports >= and >
type BoundedCounterCrdt struct {
	CRDTVM
	permissions map[int16]int32
	decs        map[int16]int32
	limit       int32 //The value that must always be kept
	replicaID   int16
	value       int32
}

//Both update and downstream
type SetCounterBound struct {
	Bound        int32
	CompEq       bool
	InitialValue int32
}

//Both update and downstream
type TransferCounter struct {
	ToTransfer  int32
	FromID      int16
	ToID        int16
	ToReplicate *bool
}

//New inc/dec downstream to know the source replica
type DownstreamIncBCounter struct {
	Change    int32
	ReplicaID int16
}

type DownstreamDecBCounter struct {
	Change      int32
	ReplicaID   int16
	ToReplicate *bool
}

func (crdt *BoundedCounterCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_FATCOUNTER }

func (args SetCounterBound) GetCRDTType() proto.CRDTType { return proto.CRDTType_FATCOUNTER }

func (args SetCounterBound) MustReplicate() bool { return true }

func (args TransferCounter) GetCRDTType() proto.CRDTType { return proto.CRDTType_FATCOUNTER }

func (args TransferCounter) MustReplicate() bool { return *args.ToReplicate }

func (args DownstreamIncBCounter) GetCRDTType() proto.CRDTType { return proto.CRDTType_FATCOUNTER }

func (args DownstreamIncBCounter) MustReplicate() bool { return true }

func (args DownstreamDecBCounter) GetCRDTType() proto.CRDTType { return proto.CRDTType_FATCOUNTER }

func (args DownstreamDecBCounter) MustReplicate() bool { return *args.ToReplicate }

func (crdt *BoundedCounterCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	crdt = &BoundedCounterCrdt{
		CRDTVM:      (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		permissions: make(map[int16]int32),
		decs:        make(map[int16]int32),
		limit:       0,
		replicaID:   replicaID,
		value:       0,
	}
	ts := clocksi.NewClockSiTimestamp().(clocksi.ClockSiTimestamp)
	for id, _ := range ts.VectorClock {
		crdt.permissions[id], crdt.decs[id] = 0, 0
	}
	return crdt
}

//Used to initialize when building a CRDT from a remote snapshot
func (crdt *BoundedCounterCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *BoundedCounterCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *BoundedCounterCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	fmt.Println("[BC][Read]Value: ", crdt.value)
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return crdt.GetValue()
	}
	//TODO: Do with updsNotYetApplied... must consider limit.
	return
}

func (crdt *BoundedCounterCrdt) GetValue() (state State) {
	return CounterState{Value: crdt.value}
}

func (crdt *BoundedCounterCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	fmt.Printf("[BC][Update]Update type: %T\n", args)
	switch typedArgs := args.(type) {
	case Increment:
		return DownstreamIncBCounter{Change: typedArgs.Change, ReplicaID: crdt.replicaID}
	case Decrement:
		return DownstreamDecBCounter{Change: typedArgs.Change, ReplicaID: crdt.replicaID, ToReplicate: new(bool)}
	case SetCounterBound:
		fmt.Println("[BC][Update]SetCounterBound upd.")
		return typedArgs
	case TransferCounter:
		if typedArgs.ToTransfer > crdt.permissions[crdt.replicaID]/2 { //Too few rights to transfer
			return NoOp{}
		}
		return typedArgs
	}
	return nil
}

func (crdt *BoundedCounterCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamAgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *BoundedCounterCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	fmt.Printf("[BC][Downstream]Downstream type: %T\n", downstreamArgs)
	switch opType := downstreamArgs.(type) {
	case DownstreamIncBCounter:
		return crdt.increment(opType)
	case DownstreamDecBCounter:
		return crdt.decrement(opType)
	case SetCounterBound:
		fmt.Printf("[BC][Downstream]SetCounterBound downstream.")
		return crdt.counterInitOp(opType)
	case TransferCounter:
		return crdt.transfer(opType)
	}
	return
}

//Pre: op.Change >= 0.
func (crdt *BoundedCounterCrdt) increment(op DownstreamIncBCounter) (effect *Effect) {
	crdt.permissions[op.ReplicaID] += op.Change
	crdt.value += op.Change
	//TODO: Effects
	return nil
}

//Pre: op.Change <= 0
func (crdt *BoundedCounterCrdt) decrement(op DownstreamDecBCounter) (effect *Effect) {
	if op.Change > crdt.permissions[op.ReplicaID] {
		*op.ToReplicate = false
	} else {
		*op.ToReplicate = true
		crdt.permissions[op.ReplicaID] -= op.Change
		crdt.value -= op.Change
	}
	return nil
}

func (crdt *BoundedCounterCrdt) counterInitOp(op SetCounterBound) (effect *Effect) {
	fmt.Println("[BC][InitOp]Start")
	crdt.limit = op.Bound
	crdt.value = op.InitialValue
	sortedIDs := clocksi.NewClockSiTimestamp().GetSortedKeys()
	if !op.CompEq {
		crdt.limit++ //Idea: >= x is the same as > (x-1)
	}
	splitValue, remaining := (op.InitialValue-op.Bound)/int32(len(sortedIDs)), (op.InitialValue-op.Bound)%int32(len(sortedIDs))
	i := int32(0)
	for ; i < remaining; i++ {
		crdt.permissions[sortedIDs[i]] += splitValue + 1
	}
	for ; i < int32(len(sortedIDs)); i++ {
		crdt.permissions[sortedIDs[i]] += splitValue
	}
	fmt.Printf("[BC][InitOp]Finish. Limit. %d, Value: %d, permissions: %v\n", crdt.limit, crdt.value, crdt.permissions)
	return nil //TODO: Effect
}

func (crdt *BoundedCounterCrdt) transfer(op TransferCounter) (effect *Effect) {
	if op.FromID == crdt.replicaID && op.ToTransfer > crdt.permissions[op.FromID]/2 { //If the transfer comes from ourselves, ensure enough permissions will be left
		*op.ToReplicate = false
	} else {
		*op.ToReplicate = true
		crdt.permissions[op.FromID] -= op.ToTransfer
		crdt.permissions[op.ToID] += op.ToTransfer
	}
	return nil //TODO: Effect
}

//Called periodically. Checks if the counter needs more rights and, if so, requests
//them from the replica with the most rights.
func (crdt *BoundedCounterCrdt) RequestTransfer() (toAsk int32, askID int16) {
	//The total perms matches the value of the counter
	if crdt.permissions[crdt.replicaID] < (crdt.value / int32(len(crdt.permissions)) / 2) {
		//Find replica with the most permissions
		maxP, maxID := int32(0), int16(0)
		for id, perm := range crdt.permissions {
			if perm > maxP {
				maxP, maxID = perm, id
			}
		}
		//Ask for the min of: (remote rights - local rights) / 2 rights, crdt.value - crdt.permissions[local]
		toAsk := MinInt32((crdt.permissions[maxID]-crdt.permissions[crdt.replicaID])/2,
			(crdt.value - crdt.permissions[crdt.replicaID]))
		return toAsk, maxID
	}
	return -1, -1
}

func (crdt *BoundedCounterCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *BoundedCounterCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := BoundedCounterCrdt{
		CRDTVM:      crdt.CRDTVM.copy(),
		permissions: make(map[int16]int32),
		decs:        make(map[int16]int32),
		limit:       crdt.limit,
		replicaID:   crdt.replicaID,
		value:       crdt.value,
	}
	return &newCRDT
}

func (crdt *BoundedCounterCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *BoundedCounterCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *BoundedCounterCrdt) undoEffect(effect *Effect) {
	//TODO
}

func (crdt *BoundedCounterCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions
func (crdtOp SetCounterBound) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	pbOp := protobuf.GetBcounterop()
	crdtOp.Bound, crdtOp.InitialValue, crdtOp.CompEq = int32(pbOp.GetLimit()), int32(pbOp.GetInitialValue()), pbOp.GetCompEq()
	return crdtOp
}

func (crdtOp SetCounterBound) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Bcounterop: &proto.ApbBoundCounterUpdate{
		Limit: pb.Int64(int64(crdtOp.Bound)), InitialValue: pb.Int64(int64(crdtOp.InitialValue)), CompEq: &crdtOp.CompEq}}
}

func (crdtOp TransferCounter) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return nil
}

func (crdtOp TransferCounter) ToUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return nil
}

func (downOp DownstreamIncBCounter) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	pbCounter := protobuf.GetBcounterOp().GetInc()
	downOp.Change, downOp.ReplicaID = pbCounter.GetChange(), int16(pbCounter.GetReplicaID())
	return downOp
}

func (downOp DownstreamIncBCounter) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{BcounterOp: &proto.ProtoBCounterDownstream{
		Inc: &proto.ProtoIncBCounterDownstream{Change: pb.Int32(downOp.Change), ReplicaID: pb.Int32(int32(downOp.ReplicaID))}}}
}

func (downOp DownstreamDecBCounter) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	pbCounter := protobuf.GetBcounterOp().GetDec()
	downOp.Change, downOp.ReplicaID, downOp.ToReplicate = pbCounter.GetChange(), int16(pbCounter.GetReplicaID()), new(bool)
	return downOp
}

func (downOp DownstreamDecBCounter) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{BcounterOp: &proto.ProtoBCounterDownstream{
		Dec: &proto.ProtoDecBCounterDownstream{Change: pb.Int32(downOp.Change), ReplicaID: pb.Int32(int32(downOp.ReplicaID))}}}
}

func (downOp SetCounterBound) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	pbOp := protobuf.GetBcounterOp().GetSetBounds()
	downOp.Bound, downOp.InitialValue, downOp.CompEq = pbOp.GetLimit(), pbOp.GetValue(), pbOp.GetCompEq()
	return downOp
}

func (downOp SetCounterBound) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{BcounterOp: &proto.ProtoBCounterDownstream{
		SetBounds: &proto.ProtoSetBoundCounterDownstream{Limit: &downOp.Bound, CompEq: &downOp.CompEq, Value: &downOp.InitialValue}}}
}

func (downOp TransferCounter) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	pbOp := protobuf.GetBcounterOp().GetTransfer()
	downOp.ToTransfer, downOp.FromID, downOp.ToID, downOp.ToReplicate = pbOp.GetTransferValue(), int16(pbOp.GetFromID()), int16(pbOp.GetToID()), new(bool)
	return downOp
}

func (downOp TransferCounter) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{BcounterOp: &proto.ProtoBCounterDownstream{
		Transfer: &proto.ProtoTransferCounterDownstream{
			TransferValue: &downOp.ToTransfer, FromID: pb.Int32(int32(downOp.FromID)), ToID: pb.Int32(int32(downOp.ToID))}}}
}

func (crdt *BoundedCounterCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	permsCopy, decsCopy := make(map[int32]int32), make(map[int32]int32)
	for key, value := range crdt.permissions {
		permsCopy[int32(key)] = value
	}
	for key, value := range crdt.decs {
		decsCopy[int32(key)] = value
	}
	return &proto.ProtoState{Bcounter: &proto.ProtoBoundedCounterState{
		Permissions: permsCopy, Decs: decsCopy, Limit: pb.Int32(crdt.limit), Value: pb.Int32(crdt.value),
	}}
}

func (crdt *BoundedCounterCrdt) FromProtoState(protobuf *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	pbCounter := protobuf.GetBcounter()
	perms, decs, permsPb, decsPb := make(map[int16]int32), make(map[int16]int32), pbCounter.GetPermissions(), pbCounter.GetDecs()
	for key, value := range permsPb {
		perms[int16(key)] = value
	}
	for key, value := range decsPb {
		decs[int16(key)] = value
	}
	return (&BoundedCounterCrdt{permissions: perms, decs: decs, limit: pbCounter.GetLimit(), value: pbCounter.GetValue()}).
		initializeFromSnapshot(ts, replicaID)
}
