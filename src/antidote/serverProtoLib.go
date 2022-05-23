package antidote

import (
	"potionDB/src/proto"

	pb "github.com/golang/protobuf/proto"
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
