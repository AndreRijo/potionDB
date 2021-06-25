package antidote

import (
	"bytes"
	"clocksi"
	"crdt"
	"encoding/binary"
	"encoding/gob"
	fmt "fmt"
	"io"
	"math/rand"
	"proto"
	"tools"

	pb "github.com/golang/protobuf/proto"
)

//Contains the conversion of transaction logic and operation wrappers protobufs
//e.g., ApbReadObjects, ApbStartTransaction, etc.
//Or, basically, every antidote protobuf that isn't specific to a CRDT
//It also contains the communication logic with AntidoteDB/PotionDB

/*
INDEX:
	COMMUNICATION
	PUBLIC CONVERSION
	HELPER CONVERSION
*/

const (
	//Requests
	ConnectReplica   = 10
	ReadObjs         = 116
	Read             = 90
	StaticRead       = 91
	UpdateObjs       = 118
	StartTrans       = 119
	AbortTrans       = 120
	CommitTrans      = 121
	StaticUpdateObjs = 122
	StaticReadObjs   = 123
	ResetServer      = 12
	NewTrigger       = 14
	GetTriggers      = 15
	//Replies
	ConnectReplicaReply = 11
	OpReply             = 111
	StartTransReply     = 124
	ReadObjsReply       = 126
	CommitTransReply    = 127
	StaticReadObjsReply = 128
	ResetServerReply    = 13
	NewTriggerReply     = 16
	GetTriggersReply    = 17
	ErrorReply          = 0
)

//Used to store en/deCoders, and their buffers. Used on a per-client basis
type CodingInfo struct {
	encBuf, decBuf *bytes.Buffer
	encoder        *gob.Encoder
	decoder        *gob.Decoder
}

func (ci CodingInfo) Initialize() CodingInfo {
	encBuf, decBuf := bytes.NewBuffer(make([]byte, 0, 1000)), bytes.NewBuffer(make([]byte, 0, 1000))
	return CodingInfo{
		encBuf: encBuf, decBuf: decBuf, encoder: gob.NewEncoder(encBuf), decoder: gob.NewDecoder(decBuf),
	}
}

//Initializes enconder + encBuf only
func (ci CodingInfo) EncInitialize() CodingInfo {
	encBuf := bytes.NewBuffer(make([]byte, 0, 1000))
	return CodingInfo{encBuf: encBuf, encoder: gob.NewEncoder(encBuf)}
}

//Initializes decoder + decBuf only
func (ci CodingInfo) DecInitialize() CodingInfo {
	decBuf := bytes.NewBuffer(make([]byte, 0, 1000))
	return CodingInfo{decBuf: decBuf, decoder: gob.NewDecoder(decBuf)}
}

//: A lot of code repetition between ORMap and RRMap. Might be worth to later merge them

/*****COMMUNICATION*****/

//Every msg sent to antidote has a 5 byte uint header.
//First 4 bytes: msgSize (uint32), 5th: msg type (byte)
func SendProto(code byte, protobf pb.Message, writer io.Writer) {
	toSend, err := pb.Marshal(protobf)
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

func ReceiveProto(in io.Reader) (msgType byte, protobuf pb.Message, err error) {
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
	for nRead := 0; nRead < 4; {
		n, err = in.Read(sizeBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}

	msgSize := (int)(binary.BigEndian.Uint32(sizeBuf))
	msgBuf := make([]byte, msgSize)
	for nRead := 0; nRead < msgSize; {
		n, err = in.Read(msgBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}

	msgType = msgBuf[0]
	msgData = msgBuf[1:]
	return
}

/*****PUBLIC CONVERSION*****/

/*****REQUEST PROTOS*****/

//Note: timestamp can be nil.
func CreateStartTransaction(timestamp []byte) (protoBuf *proto.ApbStartTransaction) {
	transProps := &proto.ApbTxnProperties{
		ReadWrite: pb.Uint32(0),
		RedBlue:   pb.Uint32(0),
	}
	protoBuf = &proto.ApbStartTransaction{
		Properties: transProps,
		Timestamp:  timestamp,
	}
	return
}

func CreateCommitTransaction(transId []byte) (protoBuf *proto.ApbCommitTransaction) {
	protoBuf = &proto.ApbCommitTransaction{
		TransactionDescriptor: transId,
	}
	return
}

func CreateAbortTransaction(transId []byte) (protoBuf *proto.ApbAbortTransaction) {
	protoBuf = &proto.ApbAbortTransaction{
		TransactionDescriptor: transId,
	}
	return
}

func CreateRead(transId []byte, fullReads []ReadObjectParams, partialReads []ReadObjectParams) (protobuf *proto.ApbRead) {
	return &proto.ApbRead{
		Fullreads:             createBoundObjectsArray(fullReads),
		Partialreads:          createPartialReads(partialReads),
		TransactionDescriptor: transId,
	}
}

func CreateStaticRead(transId []byte, fullReads []ReadObjectParams, partialReads []ReadObjectParams) (protobuf *proto.ApbStaticRead) {
	return &proto.ApbStaticRead{
		Fullreads:    createBoundObjectsArray(fullReads),
		Partialreads: createPartialReads(partialReads),
		Transaction:  CreateStartTransaction(transId),
	}
}

//: Use a struct different from the one in transactionManager.
func CreateStaticReadObjs(transId []byte, readParams []ReadObjectParams) (protobuf *proto.ApbStaticReadObjects) {
	protobuf = &proto.ApbStaticReadObjects{
		Transaction: CreateStartTransaction(transId),
		Objects:     createBoundObjectsArray(readParams),
	}
	return
}

func CreateReadObjs(transId []byte, readParams []ReadObjectParams) (protobuf *proto.ApbReadObjects) {
	protobuf = &proto.ApbReadObjects{
		TransactionDescriptor: transId,
		Boundobjects:          createBoundObjectsArray(readParams),
	}
	return
}

func CreateSingleReadObjs(transId []byte, key string, crdtType proto.CRDTType,
	bucket string) (protoBuf *proto.ApbReadObjects) {
	boundObj := &proto.ApbBoundObject{
		Key:    []byte(key),
		Type:   &crdtType,
		Bucket: []byte(bucket),
	}
	boundObjArray := make([]*proto.ApbBoundObject, 1)
	boundObjArray[0] = boundObj
	protoBuf = &proto.ApbReadObjects{
		Boundobjects:          boundObjArray,
		TransactionDescriptor: transId,
	}
	return
}

func CreateStaticUpdateObjs(transId []byte, updates []UpdateObjectParams) (protobuf *proto.ApbStaticUpdateObjects) {
	protobuf = &proto.ApbStaticUpdateObjects{
		Transaction: CreateStartTransaction(transId),
		Updates:     createUpdateOps(updates),
	}
	return
}

func CreateUpdateObjs(transId []byte, updates []UpdateObjectParams) (protobuf *proto.ApbUpdateObjects) {
	protobuf = &proto.ApbUpdateObjects{
		TransactionDescriptor: transId,
		Updates:               createUpdateOps(updates),
	}
	return
}

func CreateNewTrigger(trigger AutoUpdate, isGeneric bool, ci CodingInfo) (protobuf *proto.ApbNewTrigger) {
	return &proto.ApbNewTrigger{Source: createTriggerInfo(trigger.Trigger, ci),
		Target: createTriggerInfo(trigger.Target, ci), IsGeneric: &isGeneric}
}

func createTriggerInfo(info Link, ci CodingInfo) (protobuf *proto.ApbTriggerInfo) {
	for arg := range info.Arguments {
		ci.encoder.Encode(arg)
	}
	argBytes := ci.encBuf.Bytes()
	return &proto.ApbTriggerInfo{
		Obj:    createBoundObject(info.Key, info.CrdtType, info.Bucket),
		OpType: pb.Int32(int32(info.OpType)),
		NArgs:  pb.Int32(int32(len(info.Arguments))),
		Args:   argBytes,
	}
}

func CreateGetTriggers() (protobuf *proto.ApbGetTriggers) {
	return &proto.ApbGetTriggers{}
}

/*****REPLY/RESP PROTOS*****/

func CreateStartTransactionResp(txnId TransactionId, ts clocksi.Timestamp) (protobuf *proto.ApbStartTransactionResp) {
	protobuf = &proto.ApbStartTransactionResp{
		Success:               pb.Bool(true),
		TransactionDescriptor: createTxnDescriptorBytes(txnId, ts),
	}
	return
}

func CreateCommitOkResp(txnId TransactionId, ts clocksi.Timestamp) (protobuf *proto.ApbCommitResp) {
	protobuf = &proto.ApbCommitResp{
		Success:    pb.Bool(true),
		CommitTime: createTxnDescriptorBytes(txnId, ts),
	}
	return
}

func CreateCommitFailedResp(errorCode uint32) (protobuf *proto.ApbCommitResp) {
	protobuf = &proto.ApbCommitResp{
		Success:   pb.Bool(false),
		Errorcode: pb.Uint32(errorCode),
	}
	return
}

//: Check if these replies are being given just like in antidote (i.e., same arguments in case of success/failure, etc.)
//func CreateStaticReadResp(readReplies []*proto.ApbReadObjectResp, ts clocksi.Timestamp) (protobuf *proto.ApbStaticReadObjectsResp) {
func CreateStaticReadResp(objectStates []crdt.State, txnId TransactionId, ts clocksi.Timestamp) (protobuf *proto.ApbStaticReadObjectsResp) {
	protobuf = &proto.ApbStaticReadObjectsResp{
		Objects:    CreateReadObjectsResp(objectStates),
		Committime: CreateCommitOkResp(txnId, ts),
	}
	return
}

func CreateReadObjectsResp(objectStates []crdt.State) (protobuf *proto.ApbReadObjectsResp) {
	readReplies := convertAntidoteStatesToProto(objectStates)
	protobuf = &proto.ApbReadObjectsResp{
		Success: pb.Bool(true),
		Objects: readReplies,
	}
	return
}

func CreateOperationResp() (protobuf *proto.ApbOperationResp) {
	protobuf = &proto.ApbOperationResp{
		Success: pb.Bool(true),
	}
	return
}

func CreateNewTriggerReply() (protobuf *proto.ApbNewTriggerReply) {
	return &proto.ApbNewTriggerReply{}
}

func CreateGetTriggersReply(db *TriggerDB, ci CodingInfo) (protobuf *proto.ApbGetTriggersReply) {
	mapp, genMap := db.Mapping, db.GenericMapping
	mapSlice, genSlice := make([]*proto.ApbNewTrigger, db.getNTriggers()), make([]*proto.ApbNewTrigger, db.getNGenericTriggers())
	i, j := 0, 0
	db.DebugPrint("[APL]")
	for _, upds := range mapp {
		for _, upd := range upds {
			fmt.Println("Adding non-generic to reply")
			mapSlice[i] = CreateNewTrigger(upd, false, ci)
			i++
		}
	}
	for _, upds := range genMap {
		for _, upd := range upds {
			fmt.Println("Adding generic to reply")
			genSlice[j] = CreateNewTrigger(upd, true, ci)
			j++
		}
	}
	return &proto.ApbGetTriggersReply{Mapping: mapSlice, GenericMapping: genSlice}
}

/***** PROTO -> ANTIDOTE *****/

func DecodeTxnDescriptor(bytes []byte) (txnId TransactionId, ts clocksi.Timestamp) {
	if bytes == nil || len(bytes) == 0 {
		//FromBytes of clocksi can handle nil arrays
		txnId, ts = TransactionId(rand.Uint64()), clocksi.ClockSiTimestamp{}.FromBytes(bytes)
	} else {
		txnId, ts = TransactionId(binary.BigEndian.Uint64(bytes[0:8])), clocksi.ClockSiTimestamp{}.FromBytes(bytes[8:])
	}
	return
}

func ProtoObjectsToAntidoteObjects(protoObjs []*proto.ApbBoundObject) (objs []ReadObjectParams) {
	objs = make([]ReadObjectParams, len(protoObjs))

	for i, currObj := range protoObjs {
		objs[i] = ReadObjectParams{
			KeyParams: CreateKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{},
		}
	}
	return
}

func ProtoReadToAntidoteObjects(fullReads []*proto.ApbBoundObject, partialReads []*proto.ApbPartialRead) (objs []ReadObjectParams) {
	objs = make([]ReadObjectParams, len(fullReads)+len(partialReads))

	for i, currObj := range fullReads {
		objs[i] = ReadObjectParams{
			KeyParams: CreateKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{},
		}
	}

	var boundObj *proto.ApbBoundObject
	for i, currObj := range partialReads {
		boundObj = currObj.GetObject()
		objs[i+len(fullReads)] = ReadObjectParams{
			KeyParams: CreateKeyParams(string(boundObj.GetKey()), boundObj.GetType(), string(boundObj.GetBucket())),
			ReadArgs:  *crdt.PartialReadOpToAntidoteRead(currObj.GetArgs(), boundObj.GetType(), currObj.GetReadtype()),
		}
	}
	return
}

func ProtoUpdateOpToAntidoteUpdate(protoUp []*proto.ApbUpdateOp) (upParams []*UpdateObjectParams) {
	upParams = make([]*UpdateObjectParams, len(protoUp))
	var currObj *proto.ApbBoundObject = nil
	var currUpOp *proto.ApbUpdateOperation = nil

	for i, update := range protoUp {
		currObj, currUpOp = update.GetBoundobject(), update.GetOperation()
		upParams[i] = &UpdateObjectParams{
			KeyParams:  CreateKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			UpdateArgs: crdt.UpdateProtoToAntidoteUpdate(currUpOp, currObj.GetType()),
		}
	}

	return
}

func ProtoTriggerToAntidote(protoTrigger *proto.ApbNewTrigger, ci CodingInfo) AutoUpdate {
	return AutoUpdate{
		Trigger: ProtoTriggerInfoToAntidote(protoTrigger.Source, ci),
		Target:  ProtoTriggerInfoToAntidote(protoTrigger.Target, ci),
	}
}

func ProtoTriggerInfoToAntidote(protoTrigger *proto.ApbTriggerInfo, ci CodingInfo) Link {
	boundObj, nArgs, argsBytes := protoTrigger.GetObj(), protoTrigger.GetNArgs(), protoTrigger.GetArgs()
	ci.decBuf.Write(argsBytes)
	args := make([]interface{}, nArgs)
	for i := range args {
		var arg interface{}
		ci.decoder.Decode(arg)
		args[i] = arg
	}
	ci.decBuf.Reset()

	return Link{
		KeyParams: CreateKeyParams(string(boundObj.GetKey()), boundObj.GetType(), string(boundObj.GetBucket())),
		OpType:    OpType(protoTrigger.GetOpType()),
		Arguments: args,
	}
}

/*****HELPER CONVERSION*****/

func unmarshallProto(code byte, msgBuf []byte) (protobuf pb.Message) {
	switch code {
	case StartTrans:
		protobuf = &proto.ApbStartTransaction{}
	case ReadObjs:
		protobuf = &proto.ApbReadObjects{}
	case Read:
		protobuf = &proto.ApbRead{}
	case UpdateObjs:
		protobuf = &proto.ApbUpdateObjects{}
	case AbortTrans:
		protobuf = &proto.ApbAbortTransaction{}
	case CommitTrans:
		protobuf = &proto.ApbCommitTransaction{}
	case StaticUpdateObjs:
		protobuf = &proto.ApbStaticUpdateObjects{}
	case StaticReadObjs:
		protobuf = &proto.ApbStaticReadObjects{}
	case StaticRead:
		protobuf = &proto.ApbStaticRead{}
	case ResetServer:
		protobuf = &proto.ApbResetServer{}
	case NewTrigger:
		protobuf = &proto.ApbNewTrigger{}
	case GetTriggers:
		protobuf = &proto.ApbGetTriggers{}
	case OpReply:
		protobuf = &proto.ApbOperationResp{}
	case StartTransReply:
		protobuf = &proto.ApbStartTransactionResp{}
	case ReadObjsReply:
		protobuf = &proto.ApbReadObjectsResp{}
	case CommitTransReply:
		protobuf = &proto.ApbCommitResp{}
	case StaticReadObjsReply:
		protobuf = &proto.ApbStaticReadObjectsResp{}
	case ResetServerReply:
		protobuf = &proto.ApbResetServerResp{}
	case NewTriggerReply:
		protobuf = &proto.ApbNewTriggerReply{}
	case GetTriggersReply:
		protobuf = &proto.ApbGetTriggersReply{}
	case ErrorReply:
		protobuf = &proto.ApbErrorResp{}
	}
	//fmt.Println(code)
	err := pb.Unmarshal(msgBuf[:], protobuf)
	//fmt.Println(protobuf)
	tools.CheckErr("Error unmarshaling received protobuf", err)
	return
}

/*****GENERIC*****/

func createBoundObjectsArray(readParams []ReadObjectParams) (protobufs []*proto.ApbBoundObject) {
	protobufs = make([]*proto.ApbBoundObject, len(readParams))
	for i, param := range readParams {
		protobufs[i] = createBoundObject(param.Key, param.CrdtType, param.Bucket)
	}
	return
}

func createBoundObject(key string, crdtType proto.CRDTType, bucket string) (protobuf *proto.ApbBoundObject) {
	protobuf = &proto.ApbBoundObject{
		Key:    []byte(key),
		Type:   &crdtType,
		Bucket: []byte(bucket),
	}
	return
}

func createTxnDescriptorBytes(txnId TransactionId, ts clocksi.Timestamp) (bytes []byte) {
	tsBytes := ts.ToBytes()
	bytes = make([]byte, len(tsBytes)+8)
	binary.BigEndian.PutUint64(bytes[0:8], uint64(txnId))
	copy(bytes[8:], tsBytes)
	return
}

func convertAntidoteStatesToProto(objectStates []crdt.State) (protobufs []*proto.ApbReadObjectResp) {
	protobufs = make([]*proto.ApbReadObjectResp, len(objectStates))
	for i, state := range objectStates {
		//fmt.Printf("State to add to reply: %T %v\n", state, state)
		protobufs[i] = state.(crdt.ProtoState).ToReadResp()
		//fmt.Println("State added to reply:", protobufs[i])
	}
	return
}

/*****REQUEST PROTOS*****/

func createUpdateOps(updates []UpdateObjectParams) (protobufs []*proto.ApbUpdateOp) {
	protobufs = make([]*proto.ApbUpdateOp, len(updates))
	for i, upd := range updates {
		protobufs[i] = &proto.ApbUpdateOp{
			Boundobject: createBoundObject(upd.Key, upd.CrdtType, upd.Bucket),
			Operation:   (*upd.UpdateArgs).(crdt.ProtoUpd).ToUpdateObject(),
		}
	}
	return
}

func createPartialReads(readParams []ReadObjectParams) (protobufs []*proto.ApbPartialRead) {
	protobufs = make([]*proto.ApbPartialRead, len(readParams))
	for i, param := range readParams {
		protobufs[i] = createPartialRead(param.Key, param.CrdtType, param.Bucket, param.ReadArgs)
	}
	return
}

func createPartialRead(key string, crdtType proto.CRDTType, bucket string, readArgs crdt.ReadArguments) (protobuf *proto.ApbPartialRead) {
	readType := readArgs.GetREADType()
	return &proto.ApbPartialRead{Object: createBoundObject(key, crdtType, bucket), Readtype: &readType, Args: readArgs.(crdt.ProtoRead).ToPartialRead()}
}
