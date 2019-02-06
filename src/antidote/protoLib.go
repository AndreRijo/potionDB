package antidote

import (
	"clocksi"
	"crdt"
	"encoding/binary"
	"io"
	"tools"

	proto "github.com/golang/protobuf/proto"
)

const (
	//Requests
	ReadObjs         = 116
	UpdateObjs       = 118
	StartTrans       = 119
	AbortTrans       = 120
	CommitTrans      = 121
	StaticUpdateObjs = 122
	StaticReadObjs   = 123
	//Replies
	OpReply             = 111
	StartTransReply     = 124
	ReadObjsReply       = 126
	CommitTransReply    = 127
	StaticReadObjsReply = 128
	ErrorReply          = 0
)

//Every msg sent to antidote has a 5 byte uint header.
//First 4 bytes: msgSize (uint32), 5th: msg type (byte)
func SendProto(code byte, protobf proto.Message, writer io.Writer) {
	toSend, err := proto.Marshal(protobf)
	tools.CheckErr("Marshal err", err)
	protoSize := len(toSend)
	buffer := make([]byte, protoSize+5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = code
	copy(buffer[5:], toSend)
	_, err = writer.Write(buffer)
	tools.CheckErr("Sending protobuf err: %s\n", err)
	//fmt.Println("Protobuf sent succesfully!\n")
}

func ReceiveProto(in io.Reader) (msgType byte, protobuf proto.Message, err error) {
	msgType, msgBuf, err := readProtoFromNetwork(in)
	if err != nil {
		return
	}
	protobuf = unmarshallProto(msgType, msgBuf)
	return
}

func readProtoFromNetwork(in io.Reader) (msgType byte, msgData []byte, err error) {
	sizeBuf := make([]byte, 4)
	n := 0
	//fmt.Println("Starting to read msg size")
	for nRead := 0; nRead < 4; {
		n, err = in.Read(sizeBuf[nRead:])
		if err != nil {
			return
		}
		//tools.CheckErr("Error reading antidote's reply header", err)
		nRead += n
		//fmt.Println("Read", nRead, "bytes from msg size")
	}

	msgSize := (int)(binary.BigEndian.Uint32(sizeBuf))
	msgBuf := make([]byte, msgSize)
	//fmt.Println("MsgSize:", msgSize)
	//fmt.Println("Starting to read msg contents...")
	for nRead := 0; nRead < msgSize; {
		n, err = in.Read(msgBuf[nRead:])
		if err != nil {
			return
		}
		//tools.CheckErr("Error reading antidote's reply", err)
		nRead += n
		//fmt.Println("Read", nRead, "bytes from msg content")
	}

	msgType = msgBuf[0]
	msgData = msgBuf[1:]
	return
}

func unmarshallProto(code byte, msgBuf []byte) (protobuf proto.Message) {
	switch code {
	case StartTrans:
		protobuf = &ApbStartTransaction{}
	case ReadObjs:
		protobuf = &ApbReadObjects{}
	case UpdateObjs:
		protobuf = &ApbUpdateObjects{}
	case AbortTrans:
		protobuf = &ApbAbortTransaction{}
	case CommitTrans:
		protobuf = &ApbCommitTransaction{}
	case StaticUpdateObjs:
		protobuf = &ApbStaticUpdateObjects{}
	case StaticReadObjs:
		protobuf = &ApbStaticReadObjects{}
	case OpReply:
		protobuf = &ApbOperationResp{}
	case StartTransReply:
		protobuf = &ApbStartTransactionResp{}
	case ReadObjsReply:
		protobuf = &ApbReadObjectsResp{}
	case CommitTransReply:
		protobuf = &ApbCommitResp{}
	case StaticReadObjsReply:
		protobuf = &ApbStaticReadObjectsResp{}
	case ErrorReply:
		protobuf = &ApbErrorResp{}
	}
	//fmt.Println(code)
	err := proto.Unmarshal(msgBuf[:], protobuf)
	//fmt.Println(protobuf)
	tools.CheckErr("Error unmarshaling received protobuf", err)
	return
}

func CreateStartTransaction() (protoBuf *ApbStartTransaction) {
	transProps := &ApbTxnProperties{
		ReadWrite: proto.Uint32(0),
		RedBlue:   proto.Uint32(0),
	}
	protoBuf = &ApbStartTransaction{
		Properties: transProps,
	}
	return
}

func CreateCommitTransaction(transId []byte) (protoBuf *ApbCommitTransaction) {
	protoBuf = &ApbCommitTransaction{
		TransactionDescriptor: transId,
	}
	return
}

func CreateReadObjs(transId []byte, key string, crdtType CRDTType,
	bucket string) (protoBuf *ApbReadObjects) {
	boundObj := &ApbBoundObject{
		Key:    []byte(key),
		Type:   &crdtType,
		Bucket: []byte(bucket),
	}
	boundObjArray := make([]*ApbBoundObject, 1)
	boundObjArray[0] = boundObj
	protoBuf = &ApbReadObjects{
		Boundobjects:          boundObjArray,
		TransactionDescriptor: transId,
	}
	return
}

func CreateUpdateObjs(transId []byte, key string, crdtType CRDTType,
	bucket string, updObj proto.Message) (protoBuf *ApbUpdateObjects) {
	updateOperation := &ApbUpdateOperation{}
	boundObj := &ApbBoundObject{
		Key:    []byte(key),
		Type:   &crdtType,
		Bucket: []byte(bucket),
	}
	updateOp := &ApbUpdateOp{
		Boundobject: boundObj,
		Operation:   updateOperation,
	}
	updateOpArray := make([]*ApbUpdateOp, 1)
	updateOpArray[0] = updateOp

	switch crdtType {
	case CRDTType_TOPK:
		//fmt.Println("Creating update topk")
		converted := updObj.(*ApbTopkUpdate)
		updateOperation.Topkop = converted
		protoBuf = &ApbUpdateObjects{
			Updates:               updateOpArray,
			TransactionDescriptor: transId,
		}

	case CRDTType_COUNTER:
		//fmt.Println("Creating update counter")
		converted := updObj.(*ApbCounterUpdate)
		updateOperation.Counterop = converted
		protoBuf = &ApbUpdateObjects{
			Updates:               updateOpArray,
			TransactionDescriptor: transId,
		}
	case CRDTType_ORSET:
		converted := updObj.(*ApbSetUpdate)
		updateOperation.Setop = converted
		protoBuf = &ApbUpdateObjects{
			Updates:               updateOpArray,
			TransactionDescriptor: transId,
		}
	default:
		//fmt.Println("Didn't recognize CRDTType:", crdtType)
		protoBuf = nil
	}
	return
}

func CreateCounterUpdate(amount int) (protoBuf *ApbCounterUpdate) {
	protoBuf = &ApbCounterUpdate{
		Inc: proto.Int64(int64(amount)),
	}
	return
}

func CreateTopkUpdate(playerId int, score int) (protoBuf *ApbTopkUpdate) {
	protoBuf = &ApbTopkUpdate{
		PlayerId: proto.Int64(int64(playerId)),
		Score:    proto.Int64(int64(score)),
	}
	return
}

func CreateSetUpdate(opType ApbSetUpdate_SetOpType, elems []string) (protoBuf *ApbSetUpdate) {
	byteArray := make([][]byte, len(elems))
	for i, elem := range elems {
		byteArray[i] = []byte(elem)
	}
	switch opType {
	case ApbSetUpdate_ADD:
		protoBuf = &ApbSetUpdate{
			Optype: &opType,
			Adds:   byteArray,
		}
	case ApbSetUpdate_REMOVE:
		protoBuf = &ApbSetUpdate{
			Optype: &opType,
			Rems:   byteArray,
		}
	}
	return
}

//TODO: Use a struct different from the one in transactionManager
func CreateStaticReadObjs(readParams []ReadObjectParams) (protobuf *ApbStaticReadObjects) {
	protobuf = &ApbStaticReadObjects{
		Transaction: CreateStartTransaction(),
		Objects:     createBoundObjectsArray(readParams),
	}
	return
}

func createBoundObjectsArray(readParams []ReadObjectParams) (protobufs []*ApbBoundObject) {
	protobufs = make([]*ApbBoundObject, len(readParams))
	for i, param := range readParams {
		protobufs[i] = createBoundObject(param.Key, param.CrdtType, param.Bucket)
	}
	return
}

func createBoundObject(key string, crdtType CRDTType, bucket string) (protobuf *ApbBoundObject) {
	protobuf = &ApbBoundObject{
		Key:    []byte(key),
		Type:   &crdtType,
		Bucket: []byte(bucket),
	}
	return
}

//TODO: Check if these replies are being given just like in antidote (i.e., same arguments in case of success/failure, etc.)
//func CreateStaticReadResp(readReplies []*ApbReadObjectResp, ts clocksi.Timestamp) (protobuf *ApbStaticReadObjectsResp) {
func CreateStaticReadResp(objectStates []crdt.State, ts clocksi.Timestamp) (protobuf *ApbStaticReadObjectsResp) {
	protobuf = &ApbStaticReadObjectsResp{
		Objects:    CreateReadObjectsResp(objectStates),
		Committime: CreateCommitOkResp(ts),
	}
	return
}

func CreateCommitOkResp(ts clocksi.Timestamp) (protobuf *ApbCommitResp) {
	protobuf = &ApbCommitResp{
		Success:    proto.Bool(true),
		CommitTime: ts.ToBytes(),
	}
	return
}

func CreateCommitFailedResp(errorCode uint32) (protobuf *ApbCommitResp) {
	protobuf = &ApbCommitResp{
		Success:   proto.Bool(false),
		Errorcode: proto.Uint32(errorCode),
	}
	return
}

//TODO: Get rid of so many type conversions (this will depend on CRDT's implementation)
func createCounterReadResp(value int32) (protobuf *ApbReadObjectResp) {
	protobuf = &ApbReadObjectResp{
		Counter: &ApbGetCounterResp{
			Value: proto.Int32(value),
		},
	}
	return
}

func createSetReadResp(elems []crdt.Element) (protobuf *ApbReadObjectResp) {
	protobuf = &ApbReadObjectResp{
		Set: &ApbGetSetResp{
			Value: crdt.ElementArrayToByteMatrix(elems),
		},
	}
	return
}

//func CreateReadObjectsResp(readReplies []*ApbReadObjectResp) (protobuf *ApbReadObjectsResp) {
func CreateReadObjectsResp(objectStates []crdt.State) (protobuf *ApbReadObjectsResp) {
	readReplies := convertAntidoteStatesToProto(objectStates)
	protobuf = &ApbReadObjectsResp{
		Success: proto.Bool(true),
		Objects: readReplies,
	}
	return
}

//TODO: Use a different struct from the one in transactionManager
func CreateStaticUpdateObjs(updates []UpdateObjectParams) (protobuf *ApbStaticUpdateObjects) {
	protobuf = &ApbStaticUpdateObjects{
		Transaction: CreateStartTransaction(),
		Updates:     createUpdateOps(updates),
	}
	return
}

func createUpdateOps(updates []UpdateObjectParams) (protobufs []*ApbUpdateOp) {
	protobufs = make([]*ApbUpdateOp, len(updates))
	for i, upd := range updates {
		protobufs[i] = &ApbUpdateOp{
			Boundobject: createBoundObject(upd.Key, upd.CrdtType, upd.Bucket),
			Operation:   createUpdateOperation(upd.UpdateArgs, upd.CrdtType),
		}
	}
	return
}

//TODO: Support the remaining CRDT types
func createUpdateOperation(updateArgs crdt.UpdateArguments, crdtType CRDTType) (protobuf *ApbUpdateOperation) {
	switch crdtType {
	case CRDTType_COUNTER:
		protobuf = &ApbUpdateOperation{
			//In protobuf it's always an increment
			Counterop: &ApbCounterUpdate{
				Inc: proto.Int64(int64(updateArgs.(crdt.Increment).Change)),
			},
		}
	case CRDTType_ORSET:
		switch convertedArgs := updateArgs.(type) {
		case crdt.Add:
			element := convertedArgs.Element
			elements := make([][]byte, 1)
			elements[0] = []byte(element)
			opType := ApbSetUpdate_ADD
			protobuf = &ApbUpdateOperation{Setop: &ApbSetUpdate{Optype: &opType, Adds: elements}}
		case crdt.Remove:
			element := convertedArgs.Element
			elements := make([][]byte, 1)
			elements[0] = []byte(element)
			opType := ApbSetUpdate_REMOVE
			protobuf = &ApbUpdateOperation{Setop: &ApbSetUpdate{Optype: &opType, Rems: elements}}
		case crdt.AddAll:
			elements := convertedArgs.Elems
			opType := ApbSetUpdate_ADD
			protobuf = &ApbUpdateOperation{Setop: &ApbSetUpdate{Optype: &opType, Adds: crdt.ElementArrayToByteMatrix(elements)}}
		case crdt.RemoveAll:
			elements := convertedArgs.Elems
			opType := ApbSetUpdate_ADD
			protobuf = &ApbUpdateOperation{Setop: &ApbSetUpdate{Optype: &opType, Adds: crdt.ElementArrayToByteMatrix(elements)}}
		}
	default:
		tools.CheckErr("CrdtType not supported for update operation.", nil)
	}
	return
}

func convertAntidoteStatesToProto(objectStates []crdt.State) (protobufs []*ApbReadObjectResp) {
	protobufs = make([]*ApbReadObjectResp, len(objectStates))
	for i, state := range objectStates {
		switch convertedState := state.(type) {
		case crdt.CounterState:
			protobufs[i] = createCounterReadResp(convertedState.Value)
		case crdt.SetAWState:
			protobufs[i] = createSetReadResp(convertedState.Elems)
		default:
			tools.CheckErr("Unsupported data type in convertAntidoteStatesToProto", nil)
		}
	}
	return
}
