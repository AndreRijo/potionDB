package components

import (
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

func CreateS2SWrapperProto(clientID int32, msgID proto.WrapperType, msg pb.Message) *proto.S2SWrapper {
	protobuf := &proto.S2SWrapper{ClientID: &clientID, MsgID: &msgID}

	switch msgID {
	case proto.WrapperType_STATIC_READ_OBJS:
		protobuf.StaticReadObjs = msg.(*proto.ApbStaticReadObjects)
	case proto.WrapperType_STATIC_READ:
		protobuf.StaticRead = msg.(*proto.ApbStaticRead)
	case proto.WrapperType_STATIC_UPDATE:
		protobuf.StaticUpd = msg.(*proto.ApbStaticUpdateObjects)
	case proto.WrapperType_START_TXN:
		protobuf.StartTxn = msg.(*proto.ApbStartTransaction)
	case proto.WrapperType_READ_OBJS:
		protobuf.ReadObjs = msg.(*proto.ApbReadObjects)
	case proto.WrapperType_READ:
		protobuf.Read = msg.(*proto.ApbRead)
	case proto.WrapperType_UPD:
		protobuf.Upd = msg.(*proto.ApbUpdateObjects)
	case proto.WrapperType_COMMIT:
		protobuf.CommitTxn = msg.(*proto.ApbCommitTransaction)
	case proto.WrapperType_ABORT:
		protobuf.AbortTxn = msg.(*proto.ApbAbortTransaction)
	case proto.WrapperType_BC_PERMS_REQ:
		protobuf.BcPermsReq = msg.(*proto.ProtoBCPermissionsReq)
	}

	return protobuf
}

func CreateS2SWrapperReplyProto(clientID int32, msgID proto.WrapperType, msg pb.Message) *proto.S2SWrapperReply {
	protobuf := &proto.S2SWrapperReply{ClientID: &clientID, MsgID: &msgID}

	switch msgID {
	case proto.WrapperType_STATIC_READ_OBJS:
		protobuf.StaticReadObjs = msg.(*proto.ApbStaticReadObjectsResp)
	case proto.WrapperType_START_TXN:
		protobuf.StartTxn = msg.(*proto.ApbStartTransactionResp)
	case proto.WrapperType_READ_OBJS:
		protobuf.ReadObjs = msg.(*proto.ApbReadObjectsResp)
	case proto.WrapperType_UPD:
		protobuf.Upd = msg.(*proto.ApbOperationResp)
	case proto.WrapperType_COMMIT:
		protobuf.CommitTxn = msg.(*proto.ApbCommitResp)
	}

	return protobuf
}

func CreateBCPermissionsReqProto(perms []map[crdt.KeyParams]int32, reqReplicaID int16) *proto.ProtoBCPermissionsReq {
	partProtos := make([]*proto.ProtoBCPermissionsPartReq, len(perms))
	for partID, partPerms := range perms {
		partProtos[partID] = CreateBCPermissionsPartReqProto(partPerms)
	}
	return &proto.ProtoBCPermissionsReq{PartReqs: partProtos, ReqReplicaID: pb.Int32(int32(reqReplicaID))}
}

func CreateBCPermissionsPartReqProto(partPerms map[crdt.KeyParams]int32) *proto.ProtoBCPermissionsPartReq {
	perms := make([]*proto.ProtoBCPermissionsPair, len(partPerms))
	i := 0
	for keyP, value := range partPerms {
		perms[i] = CreateBCPermissionsPairProto(keyP, value)
		i++
	}
	return &proto.ProtoBCPermissionsPartReq{Pairs: perms}
}

func CreateBCPermissionsPairProto(key crdt.KeyParams, value int32) *proto.ProtoBCPermissionsPair {
	return &proto.ProtoBCPermissionsPair{KeyParams: createBoundObject(key.Key, key.CrdtType, key.Bucket),
		Value: pb.Int32(value)}
}

func ProtoBCPermissionsReqToTM(protobuf *proto.ProtoBCPermissionsReq) TMBCPermsArgs {
	partProtos := protobuf.GetPartReqs()
	var pairProtos []*proto.ProtoBCPermissionsPair
	perms := make([]map[crdt.KeyParams]int32, len(partProtos))
	for i, partP := range partProtos {
		pairProtos = partP.GetPairs()
		perms[i] = make(map[crdt.KeyParams]int32)
		for _, pair := range pairProtos {
			perms[i][fromBoundObjectToKeyParams(pair.GetKeyParams())] = pair.GetValue()
		}
	}
	return TMBCPermsArgs{Perms: perms, ReqReplicaID: int16(protobuf.GetReqReplicaID())}
}

/*
message ProtoBCPermissionsReq {
    repeated ProtoBCPermissionsPartReq partReqs = 1;
}

message ProtoBCPermissionsPartReq {
    map<uint64, int32> perms = 1;
}

optional ProtoBCPermissionsReq bcPermsReq = 11;
*/
