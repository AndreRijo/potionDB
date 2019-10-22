package antidote

import (
	"clocksi"
	"crdt"
	"proto"

	pb "github.com/golang/protobuf/proto"
)

//TODO: Define here the generic conversions for the replicator protobufs

/*****ANTIDOTE -> PROTO*****/

func createProtoStableClock(replicaID int16, ts int64) (protobuf *proto.ProtoStableClock) {
	return &proto.ProtoStableClock{SenderID: pb.Int32(int32(replicaID)), ReplicaTs: &ts}
}

func createProtoReplicatePart(replicaID int16, partitionID int64, timestamp clocksi.Timestamp, upds []*UpdateObjectParams) (protobuf *proto.ProtoReplicatePart) {
	return &proto.ProtoReplicatePart{
		SenderID:    pb.Int32(int32(replicaID)),
		PartitionID: &partitionID,
		Txn: &proto.ProtoRemoteTxn{
			Timestamp: timestamp.ToBytes(),
			Upds:      createProtoDownstreamUpds(upds),
		},
	}
}

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

/*****PROTO -> ANTIDOTE*****/

func protoToStableClock(protobuf *proto.ProtoStableClock) (stableClk *StableClock) {
	return &StableClock{SenderID: int16(protobuf.GetSenderID()), Ts: protobuf.GetReplicaTs()}
}

func protoToReplicatorRequest(protobuf *proto.ProtoReplicatePart) (request *NewReplicatorRequest) {
	return &NewReplicatorRequest{
		PartitionID: protobuf.GetPartitionID(),
		SenderID:    int16(protobuf.GetSenderID()),
		Timestamp:   clocksi.ClockSiTimestamp{}.FromBytes(protobuf.Txn.Timestamp),
		Upds:        protoToDownstreamUpds(protobuf.Txn.Upds),
	}
}

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
