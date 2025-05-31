package components

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	fmt "fmt"
	"io"
	"math/rand"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	tools "potionDB/potionDB/utilities"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
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
	ConnectReplica      = 10
	ReadObjs            = 116
	Read                = 90
	StaticRead          = 91
	UpdateObjs          = 118
	StartTrans          = 119
	AbortTrans          = 120
	CommitTrans         = 121
	StaticUpdateObjs    = 122
	StaticReadObjs      = 123
	ResetServer         = 12
	NewTrigger          = 14
	GetTriggers         = 15
	ServerConn          = 80
	S2S                 = 81
	ServerConnReplicaID = 82
	SQLString           = 18
	SQLTyped            = 19
	MultiConnect        = 20
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
	S2SReply            = 181
	MultiConnectReply   = 21
	ErrorReply          = 0
)

//var marshalOptions = proto.MarshalOptions{Deterministic: false, AllowPartial: true}

// Used to store en/deCoders, and their buffers. Used on a per-client basis
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

// Initializes enconder + encBuf only
func (ci CodingInfo) EncInitialize() CodingInfo {
	encBuf := bytes.NewBuffer(make([]byte, 0, 1000))
	return CodingInfo{encBuf: encBuf, encoder: gob.NewEncoder(encBuf)}
}

// Initializes decoder + decBuf only
func (ci CodingInfo) DecInitialize() CodingInfo {
	decBuf := bytes.NewBuffer(make([]byte, 0, 1000))
	return CodingInfo{decBuf: decBuf, decoder: gob.NewDecoder(decBuf)}
}

//: A lot of code repetition between ORMap and RRMap. Might be worth to later merge them

/*****COMMUNICATION*****/

// Every msg sent to antidote has a 5 byte uint header.
// First 4 bytes: msgSize (uint32), 5th: msg type (byte)
func SendProto(code byte, protobf pb.Message, writer io.Writer) {
	err := SendProtoNoCheck(code, protobf, writer)
	tools.CheckErr("Sending protobuf err:", err)
	//fmt.Println("Protobuf sent succesfully!\n")
}

/*func SendProtoMarshal(code byte, marshalProto []byte, writer io.Writer) error {
	protoSize := len(marshalProto)
	buffer := make([]byte, protoSize+5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = code
	copy(buffer[5:], marshalProto)
	_, err := writer.Write(buffer)
	return err
}*/

// Debug/testing method. Returns a buffer of the marshalled protobuf + code and size bytes
func GetProtoMarshal(protobf pb.Message) []byte {
	data, err := pb.Marshal(protobf)
	tools.CheckErr("Marshal err", err)
	protoSize := len(data)
	buffer := make([]byte, protoSize+5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = StaticReadObjsReply
	copy(buffer[5:], data)
	return buffer
}

// Exposes the error on sending instead of crashing
func SendProtoNoCheck(code byte, protobf pb.Message, writer io.Writer) error {
	//tsStart := time.Now().UnixNano()
	toSend, err := pb.Marshal(protobf)
	/*diff := (time.Now().UnixNano() - tsStart) / int64(time.Duration(time.Microsecond))
	if code == StaticReadObjsReply {
		fmt.Printf("[Protolib]Protobuf marshal took %d microseconds.\n", diff)
	}*/
	tools.CheckErr("Marshal err", err)
	protoSize := len(toSend)
	buffer := make([]byte, protoSize+5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = code
	copy(buffer[5:], toSend)
	//tsStart = time.Now().UnixNano()
	_, err = writer.Write(buffer)
	/*diff = (time.Now().UnixNano() - tsStart) / int64(time.Duration(time.Microsecond))
	if code == StaticReadObjsReply {
		fmt.Printf("[Protolib]Sending protobuf took %d microseconds.\n", diff)
	}*/
	//pbSize := pb.Size(protobf)
	//pb.MarshalAppend([]byte{}, protobf)
	/*buffer := make([]byte, 5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = code
	_, err = writer.Write(buffer)
	_, err = writer.Write(toSend)*/
	return err
}

func SendProtoMultiClient(code byte, client uint16, protobf pb.Message, writer io.Writer) {
	err := SendProtoMultiClientNoCheck(code, client, protobf, writer)
	tools.CheckErr("Sending multi-client protobuf err:", err)
}

func SendProtoMultiClientNoCheck(code byte, client uint16, protobf pb.Message, writer io.Writer) (err error) {
	toSend, err := pb.Marshal(protobf)
	tools.CheckErr("Marshal err", err)
	protoSize := len(toSend)
	buffer := make([]byte, protoSize+7)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+3))
	buffer[4] = code
	binary.BigEndian.PutUint16(buffer[5:7], client)
	copy(buffer[7:], toSend)
	_, err = writer.Write(buffer)
	return err
}

func ReceiveProto(in io.Reader) (msgType byte, protobuf pb.Message, err error) {
	msgType, msgBuf, err := readProtoFromNetwork(in)

	if err != nil {
		//fmt.Printf("[WARNING]Returning error on ReceiveProto. MsgType: %v, msgBuf: %v, err: %s\n", msgType, msgBuf, err)
		if err != io.EOF {
			fmt.Printf("[WARNING]Returning error on ReceiveProto. MsgType: %v, msgBuf: %v, err: %s\n", msgType, msgBuf, err)
		}
		//debug.PrintStack()
		return
	}
	//NEW
	//tools.CheckErr("Receiving proto err:", err)
	//tsStart := time.Now().UnixNano()
	protobuf = unmarshallProto(msgType, msgBuf)
	/*tsEnd := time.Now().UnixNano()
	diffTime := (tsEnd - tsStart) / int64(time.Duration(time.Microsecond))
	//if diffTime > 100 {
	if msgType == StaticReadObjsReply {
		fmt.Printf("[Protolib]Protobuf unmarshall took %d microseconds.\n", diffTime)
	}*/
	return
}

// TODO: Delete?
func ReceiveProtoNoProcess(in io.Reader) {
	readProtoFromNetwork(in)
}

func readProtoFromNetwork(in io.Reader) (msgType byte, msgData []byte, err error) {
	sizeBuf := make([]byte, 5)
	n := 0
	for nRead := 0; nRead < 5; {
		n, err = in.Read(sizeBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}
	msgSize := (int)(binary.BigEndian.Uint32(sizeBuf[:4]))
	msgType = sizeBuf[4]
	msgBuf := make([]byte, msgSize-1)
	for nRead := 0; nRead < msgSize-1; {
		n, err = in.Read(msgBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}
	msgData = msgBuf
	return
}

func ReceiveProtoMultiClient(in io.Reader) (msgType byte, client uint16, protobuf pb.Message, err error) {
	var msgData []byte
	msgType, client, msgData, err = readProtoFromNetworkMultiClient(in)

	if err != nil {
		if err != io.EOF {
			fmt.Printf("[WARNING]Returning error on ReceiveProtoMultiClient. MsgType: %v, protobuf: %v, err: %s\n", msgType, protobuf, err)
		}
		return
	}

	protobuf = unmarshallProto(msgType, msgData)
	return
}

func readProtoFromNetworkMultiClient(in io.Reader) (msgType byte, client uint16, msgData []byte, err error) {
	sizeBuf := make([]byte, 7)
	n := 0
	for nRead := 0; nRead < 7; {
		n, err = in.Read(sizeBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}
	msgSize := (int)(binary.BigEndian.Uint32(sizeBuf[:4]))
	msgType = sizeBuf[4]
	client = binary.BigEndian.Uint16(sizeBuf[5:7])
	msgBuf := make([]byte, msgSize-3)
	for nRead := 0; nRead < msgSize-3; {
		n, err = in.Read(msgBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}
	msgData = msgBuf
	return
}

/*****PUBLIC CONVERSION*****/

/*****REQUEST PROTOS*****/

// Note: timestamp can be nil.
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

func CreateRead(transId []byte, fullReads []crdt.ReadObjectParams, partialReads []crdt.ReadObjectParams) (protobuf *proto.ApbRead) {
	return &proto.ApbRead{
		Fullreads:             createBoundObjectsArray(fullReads),
		Partialreads:          createPartialReads(partialReads),
		TransactionDescriptor: transId,
	}
}

func CreateStaticRead(transId []byte, fullReads []crdt.ReadObjectParams, partialReads []crdt.ReadObjectParams) (protobuf *proto.ApbStaticRead) {
	return &proto.ApbStaticRead{
		Fullreads:    createBoundObjectsArray(fullReads),
		Partialreads: createPartialReads(partialReads),
		Transaction:  CreateStartTransaction(transId),
	}
}

// : Use a struct different from the one in transactionManager.
func CreateStaticReadObjs(transId []byte, readParams []crdt.ReadObjectParams) (protobuf *proto.ApbStaticReadObjects) {
	protobuf = &proto.ApbStaticReadObjects{
		Transaction: CreateStartTransaction(transId),
		Objects:     createBoundObjectsArray(readParams),
	}
	return
}

func CreateReadObjs(transId []byte, readParams []crdt.ReadObjectParams) (protobuf *proto.ApbReadObjects) {
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

func CreateStaticUpdateObjs(transId []byte, updates []crdt.UpdateObjectParams) (protobuf *proto.ApbStaticUpdateObjects) {
	protobuf = &proto.ApbStaticUpdateObjects{
		Transaction: CreateStartTransaction(transId),
		Updates:     createUpdateOps(updates),
	}
	return
}

func CreateUpdateObjs(transId []byte, updates []crdt.UpdateObjectParams) (protobuf *proto.ApbUpdateObjects) {
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

func CreateServerConn() (protobuf *proto.ApbServerConn) {
	return &proto.ApbServerConn{}
}

func CreateServerConnReplicaID(replicaID int16, buckets []string, serverIP string) *proto.ApbServerConnReplicaID {
	return &proto.ApbServerConnReplicaID{ReplicaID: pb.Int32(int32(replicaID)), MyBuckets: buckets, MyIP: &serverIP}
}

func CreateMultiClientConn(nClients int) (protobuf *proto.ApbMultiClientConnect) {
	return &proto.ApbMultiClientConnect{NClients: pb.Uint32(uint32(nClients))}
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

// : Check if these replies are being given just like in antidote (i.e., same arguments in case of success/failure, etc.)
// func CreateStaticReadResp(readReplies []*proto.ApbReadObjectResp, ts clocksi.Timestamp) (protobuf *proto.ApbStaticReadObjectsResp) {
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

func CreateMultiClientConnReply() (protobuf *proto.ApbMultiClientConnectResp) {
	return &proto.ApbMultiClientConnectResp{}
}

/***** PROTO -> ANTIDOTE *****/

func DecodeTxnDescriptor(bytes []byte) (txnId TransactionId, ts clocksi.Timestamp) {
	if len(bytes) == 0 {
		//FromBytes of clocksi can handle nil arrays
		txnId, ts = TransactionId(rand.Uint64()), clocksi.ClockSiTimestamp{}.FromBytes(bytes)
	} else {
		txnId, ts = TransactionId(binary.BigEndian.Uint64(bytes[0:8])), clocksi.ClockSiTimestamp{}.FromBytes(bytes[8:])
	}
	return
}

func ProtoObjectsToAntidoteObjects(protoObjs []*proto.ApbBoundObject) (objs []crdt.ReadObjectParams) {
	objs = make([]crdt.ReadObjectParams, len(protoObjs))

	for i, currObj := range protoObjs {
		objs[i] = crdt.ReadObjectParams{
			KeyParams: crdt.MakeKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{},
		}
	}
	return
}

func ProtoReadToAntidoteObjects(fullReads []*proto.ApbBoundObject, partialReads []*proto.ApbPartialRead) (objs []crdt.ReadObjectParams) {
	objs = make([]crdt.ReadObjectParams, len(fullReads)+len(partialReads))

	for i, currObj := range fullReads {
		objs[i] = crdt.ReadObjectParams{
			KeyParams: crdt.MakeKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{},
		}
	}

	var boundObj *proto.ApbBoundObject
	for i, currObj := range partialReads {
		boundObj = currObj.GetObject()
		objs[i+len(fullReads)] = crdt.ReadObjectParams{
			KeyParams: crdt.MakeKeyParams(string(boundObj.GetKey()), boundObj.GetType(), string(boundObj.GetBucket())),
			ReadArgs:  *crdt.PartialReadOpToAntidoteRead(currObj.GetArgs(), boundObj.GetType(), currObj.GetReadtype()),
		}
	}
	return
}

/*func ProtoPartialReadToReadObjectParams(read *proto.ApbPartialRead) crdt.ReadObjectParams {
	boundObj := read.GetObject()
	return crdt.ReadObjectParams{
		KeyParams: crdt.MakeKeyParams(string(boundObj.GetKey()), boundObj.GetType(), string(boundObj.GetBucket())),
		ReadArgs:  *crdt.PartialReadOpToAntidoteRead(read.GetArgs(), boundObj.GetType(), read.GetReadtype()),
	}
}*/

func ProtoUpdateOpToAntidoteUpdate(protoUp []*proto.ApbUpdateOp) (upParams []crdt.UpdateObjectParams) {
	upParams = make([]crdt.UpdateObjectParams, len(protoUp))
	var currObj *proto.ApbBoundObject = nil
	var currUpOp *proto.ApbUpdateOperation = nil

	for i, update := range protoUp {
		currObj, currUpOp = update.GetBoundobject(), update.GetOperation()
		upParams[i] = crdt.UpdateObjectParams{
			KeyParams:  crdt.MakeKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
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
		KeyParams: crdt.MakeKeyParams(string(boundObj.GetKey()), boundObj.GetType(), string(boundObj.GetBucket())),
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
	case ServerConn:
		protobuf = &proto.ApbServerConn{}
	case S2S:
		protobuf = &proto.S2SWrapper{}
	case MultiConnect:
		protobuf = &proto.ApbMultiClientConnect{}
	case ServerConnReplicaID:
		protobuf = &proto.ApbServerConnReplicaID{}
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
	case S2SReply:
		protobuf = &proto.S2SWrapperReply{}
	case MultiConnectReply:
		protobuf = &proto.ApbMultiClientConnectResp{}
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

func createBoundObjectsArray(readParams []crdt.ReadObjectParams) (protobufs []*proto.ApbBoundObject) {
	protobufs = make([]*proto.ApbBoundObject, len(readParams))
	for i, param := range readParams {
		protobufs[i] = createBoundObject(param.Key, param.CrdtType, param.Bucket)
	}
	return
}

func createBoundObject(key string, crdtType proto.CRDTType, bucket string) (protobuf *proto.ApbBoundObject) {
	return &proto.ApbBoundObject{Key: []byte(key), Type: &crdtType, Bucket: []byte(bucket)}
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

func fromBoundObjectToKeyParams(boundObj *proto.ApbBoundObject) crdt.KeyParams {
	return crdt.KeyParams{Key: string(boundObj.GetKey()), CrdtType: boundObj.GetType(), Bucket: string(boundObj.GetBucket())}
}

/*****REQUEST PROTOS*****/

func createUpdateOps(updates []crdt.UpdateObjectParams) (protobufs []*proto.ApbUpdateOp) {
	protobufs = make([]*proto.ApbUpdateOp, len(updates))
	for i, upd := range updates {
		protobufs[i] = &proto.ApbUpdateOp{
			Boundobject: createBoundObject(upd.Key, upd.CrdtType, upd.Bucket),
			Operation:   (upd.UpdateArgs).(crdt.ProtoUpd).ToUpdateObject(),
		}
	}
	return
}

func createPartialReads(readParams []crdt.ReadObjectParams) (protobufs []*proto.ApbPartialRead) {
	protobufs = make([]*proto.ApbPartialRead, len(readParams))
	for i, param := range readParams {
		protobufs[i] = createPartialRead(param.Key, param.CrdtType, param.Bucket, param.ReadArgs)
	}
	return
}

func createPartialRead(key string, crdtType proto.CRDTType, bucket string, readArgs crdt.ReadArguments) (protobuf *proto.ApbPartialRead) {
	readType := readArgs.GetREADType()
	if readType == proto.READType_FULL {
		return &proto.ApbPartialRead{Object: createBoundObject(key, crdtType, bucket), Readtype: &readType, Args: &proto.ApbPartialReadArgs{}}
	}
	return &proto.ApbPartialRead{Object: createBoundObject(key, crdtType, bucket), Readtype: &readType, Args: readArgs.(crdt.ProtoRead).ToPartialRead()}
}
