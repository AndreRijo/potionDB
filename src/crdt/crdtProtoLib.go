//TODO: I need to move protobufs to a separate package (proto) in order for this to work. That should be fine, as the protobufs only refer to basic and generated types, and never to antidote or crdts.

package crdt

/*
//This file contains the conversion to and from protobufs of every CRDT.
//The CRDT itself doesn't have to implement any interface from here. Neither do effects or other internal structures besides the ones mentioned below
//The update operations have to implement ???
//Read operations apart from StateReadArguments have to implement ProtoCompRead. StateReadArguments should implement another one?
//States have to implement ???
//Some entity will need to implement the conversion from protobufs to state/ops.


interface ProtoCompUpd {
	toUpdateObject() (protobuf *proto.ApbUpdateOperation)

	//fromUpdateObject(protobuf *proto.ApbUpdateOperation)
}

interface ProtoCompRead {
	toPartialRead() (protobuf *proto.ApbPartialReadArgs, readType proto.READType)

	//fromPartialRead(protobuf *proto.ApbPartialReadArgs, readType proto.READType) //Likely doesn't need the latter
}

interface ProtoCompState {
	toReadResp() (protobuf *proto.ApbReadObjectResp)

	//fromReadResp(proto *ApbReadObjectResp, crdtType proto.CRDTType, partReadType READType)
}

updateProtoToAntidoteUpdate(protobuf *proto.ApbUpdateOperation, crdtType proto.CRDTType) (op *crdt.UpdateArguments) {
	var tmpUpd crdt.UpdateArguments = crdt.NoOp{}

	switch crdtType {
		case proto.CRDTType_COUNTER:
			tmpUpd = crdt.Increment{}.FromUpdateObject(protobuf)
		case proto.CRDTType_LWWREG:
			tmpUpd = crdt.SetValue{}.FromUpdateObject(protobuf)
		case proto.CRDTType_ORSET:

		case proto.CRDTType_ORMAP:
			tmpUpd = crdt.SetValue{}.FromUpdateObject(protobuf)
		case proto.CRDTType_RRMAP:
			tmpUpd = crdt.SetValue{}.FromUpdateObject(protobuf)
		case proto.CRDTType_TOPK_RMV:
			tmpUpd = crdt.SetValue{}.FromUpdateObject(protobuf)
		case proto.CRDTType_AVG:
			tmpUpd = crdt.SetValue{}.FromUpdateObject(protobuf)
		case proto.CRDTType_MAXMIN:
			tmpUpd = crdt.SetValue{}.FromUpdateObject(protobuf)
		default:
			//TODO: Support other types and error case, and return error to client
		tools.CheckErr("Unsupported data type for update - we should warn the user about this one day.", nil)
	}

	return &tmpUpd
}

partialReadOpToAntidoteRead(protobuf *proto.ApbPartialReadArgs, crdtType proto.CRDTType, readType proto.READType) (read *crdt.ReadArguments) {
	//TODO: I forgot to do this in protoLib!
}

readRespProtoToAntidoteState(protobuf *proto.ApbReadObjectsResp, crdtType proto.CRDTType) (state crdt.State) {
	//ConvertProtoObjectToAntidoteState
}
*/
