package antidote

import (
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
	"potionDB/src/proto"

	pb "github.com/golang/protobuf/proto"
)

/*****ANTIDOTE -> PROTO*****/

func createProtoStableClock(replicaID int16, ts int64) (protobuf *proto.ProtoStableClock) {
	return &proto.ProtoStableClock{SenderID: pb.Int32(int32(replicaID)), ReplicaTs: &ts}
}

func createProtoReplicateGroupTxn(replicaID int16, allTxns []RemoteTxn, bucketOps []RemoteTxn) (protobuf *proto.ProtoReplicateGroupTxn) {
	protos := make([]*proto.ProtoReplicateTxn, len(bucketOps))
	for i, txn := range bucketOps {
		protos[i] = createProtoReplicateTxn(replicaID, txn.Clk, txn.Upds, txn.TxnID)
	}
	minID, maxID := allTxns[0].TxnID, allTxns[len(allTxns)-1].TxnID
	return &proto.ProtoReplicateGroupTxn{SenderID: pb.Int32(int32(replicaID)), Txns: protos, MinTxnID: pb.Int32(minID), MaxTxnID: pb.Int32(maxID)}
}

/*func createProtoReplicateGroupTxn(replicaID int16, txns []RemoteTxn, bucketOps []map[int][]*UpdateObjectParams, txnCount int32) (protobuf *proto.ProtoReplicateGroupTxn) {
	protos := make([]*proto.ProtoReplicateTxn, len(txns))
	j := 0
	initialCount := txnCount
	for i := 0; i < len(txns); i, txnCount = i+1, txnCount+1 {
		if bucketOps[i] == nil {
			continue
		} else {
			protos[j] = createProtoReplicateTxn(replicaID, txns[i].Clk, bucketOps[i], txnCount)
			j++
		}
	}
	protos = protos[:j]
	return &proto.ProtoReplicateGroupTxn{SenderID: pb.Int32(int32(replicaID)), Txns: protos, MinTxnID: pb.Int32(initialCount), MaxTxnID: pb.Int32(txnCount)}
}*/

func createProtoReplicateTxn(replicaID int16, clk clocksi.Timestamp, upds map[int][]*UpdateObjectParams, txnCount int32) (protobuf *proto.ProtoReplicateTxn) {
	protos := make([]*proto.ProtoNewRemoteTxn, len(upds))
	i := 0
	for partID, partUpds := range upds {
		protos[i] = createProtoNewRemoteTxn(int64(partID), partUpds)
		i++
	}
	return &proto.ProtoReplicateTxn{SenderID: pb.Int32(int32(replicaID)), Timestamp: clk.ToBytes(), Part: protos, TxnID: &txnCount}
}

func createProtoNewRemoteTxn(partID int64, upds []*UpdateObjectParams) (protobuf *proto.ProtoNewRemoteTxn) {
	return &proto.ProtoNewRemoteTxn{PartitionID: &partID, Upds: createProtoDownstreamUpds(upds)}
}

/*
func createProtoReplicatePart(replicaID int16, partitionID int64, timestamp clocksi.Timestamp, upds []*UpdateObjectParams, txnCount int32) (protobuf *proto.ProtoReplicatePart) {
	return &proto.ProtoReplicatePart{
		SenderID:    pb.Int32(int32(replicaID)),
		PartitionID: &partitionID,
		Txn:         &proto.ProtoRemoteTxn{Timestamp: timestamp.ToBytes(), Upds: createProtoDownstreamUpds(upds)},
		TxnID:       &txnCount,
	}
}
*/

func createProtoDownstreamUpds(upds []*UpdateObjectParams) (protobufs []*proto.ProtoDownstreamUpd) {
	protobufs = make([]*proto.ProtoDownstreamUpd, len(upds))
	for i, upd := range upds {
		protobufs[i] = &proto.ProtoDownstreamUpd{
			KeyParams: createBoundObject(upd.Key, upd.CrdtType, upd.Bucket),
			Op:        (*upd.UpdateArgs).(crdt.ProtoDownUpd).ToReplicatorObj(),
		}
	}
	return protobufs
}

func createProtoRemoteID(replicaID int16, myBuckets []string, myIP string) (protobuf *proto.ProtoRemoteID) {
	return &proto.ProtoRemoteID{ReplicaID: pb.Int32(int32(replicaID)), MyBuckets: myBuckets, MyIP: &myIP}
}

func createProtoJoin(buckets []string, replicaID int16, ip string) (protobuf *proto.ProtoJoin) {
	return &proto.ProtoJoin{Buckets: buckets, ReplicaID: pb.Int32(int32(replicaID)), ReplicaIP: &ip}
}

func createProtoReplyJoin(req ReplyJoin) (protobuf *proto.ProtoReplyJoin) {
	return &proto.ProtoReplyJoin{Buckets: req.CommonBkts, PartsClk: clocksi.ClockArrayToByteArray(req.Clks),
		ReplicaID: pb.Int32(int32(req.SenderID)), ReplicaIP: &req.ReqIP}
}

func createProtoRequestBucket(req RequestBucket) (protobuf *proto.ProtoRequestBucket) {
	return &proto.ProtoRequestBucket{Buckets: req.Buckets, ReplicaID: pb.Int32(int32(req.SenderID)), ReplicaIP: &req.ReqIP}
}

func createProtoReplyEmpty() (protobuf *proto.ProtoReplyEmpty) {
	return &proto.ProtoReplyEmpty{}
}

func createProtoReplyBucket(req ReplyBucket) (protobuf *proto.ProtoReplyBucket) {
	protoParts := make([]*proto.ProtoPartition, len(req.PartStates))
	for i, part := range req.PartStates {
		protoParts[i] = createProtoPartitions(part)
	}
	return &proto.ProtoReplyBucket{ReplicaID: pb.Int32(int32(req.SenderID)), Clk: req.Clk.ToBytes(), Parts: protoParts}
}

func createProtoPartitions(partCRDTs []*proto.ProtoCRDT) (protobuf *proto.ProtoPartition) {
	return &proto.ProtoPartition{States: partCRDTs}
}

/*****PROTO -> ANTIDOTE*****/

func protoToStableClock(protobuf *proto.ProtoStableClock) (stableClk *StableClock) {
	return &StableClock{SenderID: int16(protobuf.GetSenderID()), Ts: protobuf.GetReplicaTs()}
}

func protoToRemoteTxnGroup(protobuf *proto.ProtoReplicateGroupTxn) (request *RemoteTxnGroup) {
	protoTxns := protobuf.GetTxns()
	txns := make([]RemoteTxn, len(protoTxns))
	for i, protoTxn := range protoTxns {
		txns[i] = *protoToRemoteTxn(protoTxn)
	}
	return &RemoteTxnGroup{Txns: txns, SenderID: int16(protobuf.GetSenderID()),
		MaxTxnID: protobuf.GetMaxTxnID(), MinTxnID: protobuf.GetMinTxnID()}
}

func protoToRemoteTxn(protobuf *proto.ProtoReplicateTxn) (request *RemoteTxn) {
	protoParts := protobuf.GetPart()
	upds := make(map[int][]*UpdateObjectParams)
	for _, protoPart := range protoParts {
		partUpds, partID := protoToPartitionUpds(protoPart)
		upds[int(partID)] = partUpds
	}
	return &RemoteTxn{
		SenderID: int16(protobuf.GetSenderID()),
		Clk:      clocksi.ClockSiTimestamp{}.FromBytes(protobuf.GetTimestamp()),
		Upds:     upds,
		TxnID:    protobuf.GetTxnID(),
	}
}

func protoToPartitionUpds(protobuf *proto.ProtoNewRemoteTxn) (upds []*UpdateObjectParams, partID int64) {
	return protoToDownstreamUpds(protobuf.GetUpds()), protobuf.GetPartitionID()
}

/*
func protoToReplicatorRequest(protobuf *proto.ProtoReplicatePart) (request *NewReplicatorRequest) {
	return &NewReplicatorRequest{
		PartitionID: protobuf.GetPartitionID(),
		SenderID:    int16(protobuf.GetSenderID()),
		Timestamp:   clocksi.ClockSiTimestamp{}.FromBytes(protobuf.Txn.Timestamp),
		Upds:        protoToDownstreamUpds(protobuf.Txn.Upds),
		TxnID:       protobuf.GetTxnID(),
	}
}*/

func protoToDownstreamUpds(protobufs []*proto.ProtoDownstreamUpd) (upds []*UpdateObjectParams) {
	upds = make([]*UpdateObjectParams, len(protobufs))
	for i, protobuf := range protobufs {
		keyProto := protobuf.GetKeyParams()
		upd := &UpdateObjectParams{
			KeyParams: CreateKeyParams(string(keyProto.GetKey()), keyProto.GetType(), string(keyProto.GetBucket())),
		}
		//TODO: This should be downstream args
		var tmpArgs crdt.UpdateArguments = crdt.DownstreamProtoToAntidoteDownstream(protobuf.GetOp(), upd.CrdtType)
		upd.UpdateArgs = &tmpArgs
		upds[i] = upd
	}
	return upds
}

func protoToRemoteID(protobuf *proto.ProtoRemoteID) (remoteID int16) {
	return int16(protobuf.GetReplicaID())
}

func protoToJoin(protobuf *proto.ProtoJoin) (buckets []string, senderID int16, ip string) {
	return protobuf.GetBuckets(), int16(protobuf.GetReplicaID()), protobuf.GetReplicaIP()
}

func protoToReplyJoin(protobuf *proto.ProtoReplyJoin) (buckets []string, clks []clocksi.Timestamp, senderID int16) {
	return protobuf.GetBuckets(), clocksi.ByteArrayToClockArray(protobuf.GetPartsClk()), int16(protobuf.GetReplicaID())
}

func protoToRequestBucket(protobuf *proto.ProtoRequestBucket) (buckets []string, senderID int16) {
	return protobuf.GetBuckets(), int16(protobuf.GetReplicaID())
}

func protoToReplyBucket(protobuf *proto.ProtoReplyBucket) (states [][]*proto.ProtoCRDT, clk clocksi.Timestamp, senderID int16) {
	return protoToPartitions(protobuf.GetParts()), clocksi.ClockSiTimestamp{}.FromBytes(protobuf.GetClk()), int16(protobuf.GetReplicaID())
}

func protoToPartitions(protobuf []*proto.ProtoPartition) (states [][]*proto.ProtoCRDT) {
	states = make([][]*proto.ProtoCRDT, len(protobuf))
	for i, part := range protobuf {
		states[i] = part.GetStates()
	}
	return
}

/*
func protoToPartitions(protobufs []*proto.ProtoPartition) {
	for _, partProto := range protobufs {
		protoCRDTs := partProto.GetStates()
		for _, protoCRDT := range protoCRDTs {
			state := crdt.StateProtoToCrdt(protoCRDT.GetState(), protoCRDT.GetType())
			ignore(state)
		}
	}
}
*/

/***** AUXILIARY *****/

func apbBoundObjectToKeyParams(protobuf *proto.ApbBoundObject) (keyParams KeyParams) {
	return CreateKeyParams(string(protobuf.GetKey()), protobuf.GetType(), string(protobuf.GetBucket()))
}
