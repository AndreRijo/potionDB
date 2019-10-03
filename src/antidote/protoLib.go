package antidote

import (
	"clocksi"
	"crdt"
	"encoding/binary"
	"io"
	"math/rand"
	"tools"

	proto "github.com/golang/protobuf/proto"
)

const (
	//Requests
	ConnectReplica   = 10
	ReadObjs         = 116
	UpdateObjs       = 118
	StartTrans       = 119
	AbortTrans       = 120
	CommitTrans      = 121
	StaticUpdateObjs = 122
	StaticReadObjs   = 123
	//Replies
	ConnectReplicaReply = 11
	OpReply             = 111
	StartTransReply     = 124
	ReadObjsReply       = 126
	CommitTransReply    = 127
	StaticReadObjsReply = 128
	ErrorReply          = 0
)

//TODO: A lot of code repetition between ORMap and RRMap. Might be worth to later merge them

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

/*****REQUEST PROTOS*****/

//Note: timestamp can be nil.
func CreateStartTransaction(timestamp []byte) (protoBuf *ApbStartTransaction) {
	transProps := &ApbTxnProperties{
		ReadWrite: proto.Uint32(0),
		RedBlue:   proto.Uint32(0),
	}
	protoBuf = &ApbStartTransaction{
		Properties: transProps,
		Timestamp:  timestamp,
	}
	return
}

func CreateCommitTransaction(transId []byte) (protoBuf *ApbCommitTransaction) {
	protoBuf = &ApbCommitTransaction{
		TransactionDescriptor: transId,
	}
	return
}

func CreateAbortTransaction(transId []byte) (protoBuf *ApbAbortTransaction) {
	protoBuf = &ApbAbortTransaction{
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

//TODO: Use a struct different from the one in transactionManager.
func CreateStaticReadObjs(transId []byte, readParams []ReadObjectParams) (protobuf *ApbStaticReadObjects) {
	protobuf = &ApbStaticReadObjects{
		Transaction: CreateStartTransaction(transId),
		Objects:     createBoundObjectsArray(readParams),
	}
	return
}

func CreateReadObjsFromArray(transId []byte, readParams []ReadObjectParams) (protobuf *ApbReadObjects) {
	protobuf = &ApbReadObjects{
		TransactionDescriptor: transId,
		Boundobjects:          createBoundObjectsArray(readParams),
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

//TODO: Use a different struct from the one in transactionManager. Also, support receiving transId
func CreateStaticUpdateObjs(updates []UpdateObjectParams) (protobuf *ApbStaticUpdateObjects) {
	protobuf = &ApbStaticUpdateObjects{
		Transaction: CreateStartTransaction(nil),
		Updates:     createUpdateOps(updates),
	}
	return
}

func CreateUpdateObjsFromArray(transId []byte, updates []UpdateObjectParams) (protobuf *ApbUpdateObjects) {
	protobuf = &ApbUpdateObjects{
		TransactionDescriptor: transId,
		Updates:               createUpdateOps(updates),
	}
	return
}

func createUpdateOps(updates []UpdateObjectParams) (protobufs []*ApbUpdateOp) {
	protobufs = make([]*ApbUpdateOp, len(updates))
	for i, upd := range updates {
		protobufs[i] = &ApbUpdateOp{
			Boundobject: createBoundObject(upd.Key, upd.CrdtType, upd.Bucket),
			Operation:   createUpdateOperation(*upd.UpdateArgs, upd.CrdtType),
		}
	}
	return
}

/*****REPLY/RESP PROTOS*****/

func CreateStartTransactionResp(txnId TransactionId, ts clocksi.Timestamp) (protobuf *ApbStartTransactionResp) {
	protobuf = &ApbStartTransactionResp{
		Success:               proto.Bool(true),
		TransactionDescriptor: createTxnDescriptorBytes(txnId, ts),
	}
	return
}

func CreateCommitOkResp(txnId TransactionId, ts clocksi.Timestamp) (protobuf *ApbCommitResp) {
	protobuf = &ApbCommitResp{
		Success:    proto.Bool(true),
		CommitTime: createTxnDescriptorBytes(txnId, ts),
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

//TODO: Check if these replies are being given just like in antidote (i.e., same arguments in case of success/failure, etc.)
//func CreateStaticReadResp(readReplies []*ApbReadObjectResp, ts clocksi.Timestamp) (protobuf *ApbStaticReadObjectsResp) {
func CreateStaticReadResp(objectStates []crdt.State, txnId TransactionId, ts clocksi.Timestamp) (protobuf *ApbStaticReadObjectsResp) {
	protobuf = &ApbStaticReadObjectsResp{
		Objects:    CreateReadObjectsResp(objectStates),
		Committime: CreateCommitOkResp(txnId, ts),
	}
	return
}

func CreateReadObjectsResp(objectStates []crdt.State) (protobuf *ApbReadObjectsResp) {
	readReplies := convertAntidoteStatesToProto(objectStates)
	protobuf = &ApbReadObjectsResp{
		Success: proto.Bool(true),
		Objects: readReplies,
	}
	return
}

func CreateOperationResp() (protoBuf *ApbOperationResp) {
	protoBuf = &ApbOperationResp{
		Success: proto.Bool(true),
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

func DecodeTxnDescriptor(bytes []byte) (txnId TransactionId, ts clocksi.Timestamp) {
	if bytes == nil || len(bytes) == 0 {
		//FromBytes of clocksi can handle nil arrays
		txnId, ts = TransactionId(rand.Uint64()), clocksi.ClockSiTimestamp{}.FromBytes(bytes)
	} else {
		txnId, ts = TransactionId(binary.BigEndian.Uint64(bytes[0:8])), clocksi.ClockSiTimestamp{}.FromBytes(bytes[8:])
	}
	return
}

/***** CRDT SPECIFIC METHODS*****/

func CreateCounterUpdate(amount int) (protoBuf *ApbCounterUpdate) {
	return &ApbCounterUpdate{Inc: proto.Int64(int64(amount))}
}

func CreateRegisterUpdate(value string) (protoBuf *ApbRegUpdate) {
	return &ApbRegUpdate{Value: []byte(value)}
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

//TODO: Might want to merge the three below somehow
func CreateORMapUpdate(isAdd bool, adds map[string]crdt.Element, rems map[string]struct{}) (protoBuf *ApbMapUpdate) {
	protoBuf = &ApbMapUpdate{}
	i := 0
	crdtType := CRDTType_LWWREG
	if isAdd {
		protoBuf.Updates = make([]*ApbMapNestedUpdate, len(adds))
		for key, elem := range adds {
			protoBuf.Updates[i] = &ApbMapNestedUpdate{
				Key:    &ApbMapKey{Key: []byte(key), Type: &crdtType},
				Update: &ApbUpdateOperation{Regop: &ApbRegUpdate{Value: []byte(elem)}},
			}
			i++
		}
	} else {
		protoBuf.RemovedKeys = make([]*ApbMapKey, len(rems))
		for key := range rems {
			protoBuf.RemovedKeys[i] = &ApbMapKey{Key: []byte(key), Type: &crdtType}
			i++
		}
	}
	return
}

func CreateMapUpdate(isAdd bool, adds map[string]crdt.UpdateArguments, rems map[string]struct{}) (protoBuf *ApbMapUpdate) {
	protoBuf = &ApbMapUpdate{}
	i := 0
	if isAdd {
		protoBuf.Updates = make([]*ApbMapNestedUpdate, len(adds))
		for key, args := range adds {
			crdtType := CRDTType(args.GetCRDTType())
			protoBuf.Updates[i] = &ApbMapNestedUpdate{
				Key:    &ApbMapKey{Key: []byte(key), Type: &crdtType},
				Update: createUpdateOperation(args, crdtType),
			}
			i++
		}
	} else {
		crdtType := CRDTType_LWWREG
		protoBuf.RemovedKeys = make([]*ApbMapKey, len(rems))
		for key := range rems {
			//For now it's irrelevant the Type field
			protoBuf.RemovedKeys[i] = &ApbMapKey{Key: []byte(key), Type: &crdtType}
			i++
		}
	}
	return
}

func CreateMapUpdateFromProto(isAdd bool, adds map[string]*ApbUpdateOp, rems map[string]struct{}) (protoBuf *ApbMapUpdate) {
	protoBuf = &ApbMapUpdate{}
	i := 0
	if isAdd {
		protoBuf.Updates = make([]*ApbMapNestedUpdate, len(adds))
		for key, op := range adds {
			crdtType := op.GetBoundobject().GetType()
			protoBuf.Updates[i] = &ApbMapNestedUpdate{
				Key:    &ApbMapKey{Key: []byte(key), Type: &crdtType},
				Update: op.GetOperation(),
			}
			i++
		}
	} else {
		crdtType := CRDTType_LWWREG
		protoBuf.RemovedKeys = make([]*ApbMapKey, len(rems))
		for key := range rems {
			//For now it's irrelevant the Type field
			protoBuf.RemovedKeys[i] = &ApbMapKey{Key: []byte(key), Type: &crdtType}
			i++
		}
	}
	return
}

func CreateTopkUpdate(playerId int, score int) (protoBuf *ApbTopkUpdate) {
	return &ApbTopkUpdate{PlayerId: proto.Int64(int64(playerId)), Score: proto.Int64(int64(score))}
}

func CreateTopKRmvUpdate(isAdd bool, playerId int, score int) (protoBuf *ApbTopkRmvUpdate) {
	if isAdd {
		protoBuf = &ApbTopkRmvUpdate{Adds: make([]*ApbIntPair, 1)}
		protoBuf.Adds[0] = &ApbIntPair{PlayerId: proto.Int32(int32(playerId)), Score: proto.Int32(int32(score))}
	} else {
		protoBuf = &ApbTopkRmvUpdate{Rems: make([]int32, 1)}
		protoBuf.Rems[0] = int32(playerId)
	}
	return
}

func CreateAvgUpdate(sumValue int64, nAdds int64) (protoBuf *ApbAverageUpdate) {
	return &ApbAverageUpdate{Value: proto.Int64(sumValue), NValues: proto.Int64(nAdds)}
}

func CreateMaxMinUpdate(value int64, isMax bool) (protoBuf *ApbMaxMinUpdate) {
	return &ApbMaxMinUpdate{Value: proto.Int64(value), IsMax: proto.Bool(isMax)}
}

//TODO: Get rid of so many type conversions (this will depend on CRDT's implementation)
func createCounterReadResp(value int32) (protobuf *ApbReadObjectResp) {
	return &ApbReadObjectResp{Counter: &ApbGetCounterResp{Value: proto.Int32(value)}}
}

func createLWWRegisterReadResp(value interface{}) (protobuf *ApbReadObjectResp) {
	return &ApbReadObjectResp{Reg: &ApbGetRegResp{Value: []byte((value).(string))}}
}

func createSetReadResp(elems []crdt.Element) (protobuf *ApbReadObjectResp) {
	return &ApbReadObjectResp{Set: &ApbGetSetResp{Value: crdt.ElementArrayToByteMatrix(elems)}}
}

func createMapReadResp(entries map[string]crdt.Element) (protobuf *ApbReadObjectResp) {
	return &ApbReadObjectResp{Map: &ApbGetMapResp{Entries: entriesToApbMapEntries(entries)}}
}

func createRWMapReadResp(crdts map[string]crdt.State) (protobuf *ApbReadObjectResp) {
	return &ApbReadObjectResp{Map: &ApbGetMapResp{Entries: crdtsToApbMapEntries(crdts)}}
}

func createTopkReadResp(values []crdt.TopKScore) (protobuf *ApbReadObjectResp) {
	return &ApbReadObjectResp{Topk: &ApbGetTopkResp{Values: createApbIntPairs(values)}}
}

func createAvgReadResp(value float64) (protobuf *ApbReadObjectResp) {
	return &ApbReadObjectResp{Avg: &ApbGetAverageResp{Avg: proto.Float64(value)}}
}

func createMaxMinReadResp(value int64) (protobuf *ApbReadObjectResp) {
	return &ApbReadObjectResp{Maxmin: &ApbGetMaxMinResp{Value: proto.Int64(value)}}
}

func createApbIntPairs(values []crdt.TopKScore) (protos []*ApbIntPair) {
	protos = make([]*ApbIntPair, len(values))
	for i, score := range values {
		protos[i] = &ApbIntPair{PlayerId: proto.Int32(score.Id), Score: proto.Int32(score.Score)}
	}
	return
}

func entriesToApbMapEntries(entries map[string]crdt.Element) (protos []*ApbMapEntry) {
	protos = make([]*ApbMapEntry, len(entries))
	crdtType := CRDTType_LWWREG
	i := 0
	for key, elem := range entries {
		protos[i] = &ApbMapEntry{
			Key:   &ApbMapKey{Key: []byte(key), Type: &crdtType},
			Value: &ApbReadObjectResp{Reg: &ApbGetRegResp{Value: []byte(elem)}},
		}
		i++
	}
	return
}

func crdtsToApbMapEntries(states map[string]crdt.State) (protos []*ApbMapEntry) {
	protos = make([]*ApbMapEntry, len(states))
	i := 0
	for key, state := range states {
		crdtType := CRDTType(state.GetCRDTType())
		protos[i] = &ApbMapEntry{
			Key:   &ApbMapKey{Key: []byte(key), Type: &crdtType},
			Value: convertAntidoteStateToProto(state),
		}
		i++
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
	case CRDTType_COUNTER:
		updateOperation.Counterop = updObj.(*ApbCounterUpdate)
	case CRDTType_LWWREG:
		updateOperation.Regop = updObj.(*ApbRegUpdate)
	case CRDTType_ORSET:
		updateOperation.Setop = updObj.(*ApbSetUpdate)
	case CRDTType_ORMAP, CRDTType_RRMAP:
		updateOperation.Mapop = updObj.(*ApbMapUpdate)
	case CRDTType_TOPK:
		updateOperation.Topkop = updObj.(*ApbTopkUpdate)
	case CRDTType_TOPK_RMV:
		updateOperation.Topkrmvop = updObj.(*ApbTopkRmvUpdate)
	case CRDTType_AVG:
		updateOperation.Avgop = updObj.(*ApbAverageUpdate)
	case CRDTType_MAXMIN:
		updateOperation.Maxminop = updObj.(*ApbMaxMinUpdate)
	default:
		//fmt.Println("Didn't recognize CRDTType:", crdtType)
		return nil
	}
	return &ApbUpdateObjects{
		Updates:               updateOpArray,
		TransactionDescriptor: transId,
	}
}

func createUpdateOperation(updateArgs crdt.UpdateArguments, crdtType CRDTType) (protobuf *ApbUpdateOperation) {
	switch crdtType {
	case CRDTType_COUNTER:
		//In protobuf it's always an increment
		protobuf = &ApbUpdateOperation{Counterop: &ApbCounterUpdate{Inc: proto.Int64(int64(updateArgs.(crdt.Increment).Change))}}

	case CRDTType_LWWREG:
		protobuf = &ApbUpdateOperation{Regop: &ApbRegUpdate{Value: []byte(updateArgs.(crdt.SetValue).NewValue.(string))}}

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

	case CRDTType_ORMAP:
		//Due to being repeated fields, both entries in ApbUpdateOperation are a must
		switch convertedArgs := updateArgs.(type) {
		case crdt.MapAdd:
			entries := map[string]crdt.Element{convertedArgs.Key: convertedArgs.Value}
			protobuf = &ApbUpdateOperation{Mapop: &ApbMapUpdate{Updates: mapEntriesToProto(entries), RemovedKeys: []*ApbMapKey{}}}
		case crdt.MapRemove:
			keys := []string{convertedArgs.Key}
			protobuf = &ApbUpdateOperation{Mapop: &ApbMapUpdate{RemovedKeys: stringArrayToMapKeyArray(keys), Updates: []*ApbMapNestedUpdate{}}}
		case crdt.MapAddAll:
			entries := convertedArgs.Values
			protobuf = &ApbUpdateOperation{Mapop: &ApbMapUpdate{Updates: mapEntriesToProto(entries), RemovedKeys: []*ApbMapKey{}}}
		case crdt.MapRemoveAll:
			keys := convertedArgs.Keys
			protobuf = &ApbUpdateOperation{Mapop: &ApbMapUpdate{RemovedKeys: stringArrayToMapKeyArray(keys), Updates: []*ApbMapNestedUpdate{}}}
		}

	case CRDTType_RRMAP:
		switch convertedArgs := updateArgs.(type) {
		case crdt.EmbMapUpdate:
			entry := map[string]crdt.UpdateArguments{convertedArgs.Key: convertedArgs.Upd}
			protobuf = &ApbUpdateOperation{Mapop: &ApbMapUpdate{Updates: createMapNestedOps(entry), RemovedKeys: []*ApbMapKey{}}}
		case crdt.EmbMapUpdateAll:
			protobuf = &ApbUpdateOperation{Mapop: &ApbMapUpdate{Updates: createMapNestedOps(convertedArgs.Upds), RemovedKeys: []*ApbMapKey{}}}
		case crdt.MapRemove:
			keys := []string{convertedArgs.Key}
			protobuf = &ApbUpdateOperation{Mapop: &ApbMapUpdate{RemovedKeys: stringArrayToMapKeyArray(keys), Updates: []*ApbMapNestedUpdate{}}}
		case crdt.MapRemoveAll:
			keys := convertedArgs.Keys
			protobuf = &ApbUpdateOperation{Mapop: &ApbMapUpdate{RemovedKeys: stringArrayToMapKeyArray(keys), Updates: []*ApbMapNestedUpdate{}}}
		}

	case CRDTType_TOPK_RMV:
		switch convertedArgs := updateArgs.(type) {
		case crdt.TopKAdd:
			score := convertedArgs.TopKScore
			protobuf = &ApbUpdateOperation{Topkrmvop: &ApbTopkRmvUpdate{Adds: make([]*ApbIntPair, 1)}}
			protobuf.Topkrmvop.Adds[0] = &ApbIntPair{PlayerId: proto.Int32(int32(score.Id)), Score: proto.Int32(int32(score.Score))}
		case crdt.TopKRemove:
			protobuf = &ApbUpdateOperation{Topkrmvop: &ApbTopkRmvUpdate{Rems: make([]int32, 1)}}
			protobuf.Topkrmvop.Rems[0] = convertedArgs.Id
		}

	case CRDTType_AVG:
		switch convertedArgs := updateArgs.(type) {
		case crdt.AddValue:
			protobuf = &ApbUpdateOperation{Avgop: &ApbAverageUpdate{
				Value: proto.Int64(convertedArgs.Value), NValues: proto.Int64(1),
			}}
		case crdt.AddMultipleValue:
			protobuf = &ApbUpdateOperation{Avgop: &ApbAverageUpdate{
				Value: proto.Int64(convertedArgs.SumValue), NValues: proto.Int64(convertedArgs.NAdds),
			}}
		}
	case CRDTType_MAXMIN:
		switch convertedArgs := updateArgs.(type) {
		case crdt.MaxAddValue:
			protobuf = &ApbUpdateOperation{Maxminop: &ApbMaxMinUpdate{
				Value: proto.Int64(convertedArgs.Value), IsMax: proto.Bool(true),
			}}
		case crdt.MinAddValue:
			protobuf = &ApbUpdateOperation{Maxminop: &ApbMaxMinUpdate{
				Value: proto.Int64(convertedArgs.Value), IsMax: proto.Bool(false),
			}}
		}
	default:
		tools.CheckErr("CrdtType not supported for update operation.", nil)
	}
	return
}

func ConvertProtoUpdateToAntidote(op *ApbUpdateOperation,
	crdtType CRDTType) (updateArgs *crdt.UpdateArguments) {
	var tmpUpd crdt.UpdateArguments = crdt.NoOp{}
	switch crdtType {
	case CRDTType_COUNTER:
		tmpUpd = crdt.Increment{Change: int32(op.GetCounterop().GetInc())}
	case CRDTType_LWWREG:
		tmpUpd = crdt.SetValue{NewValue: string(op.GetRegop().GetValue())}
	case CRDTType_ORSET:
		setProto := op.GetSetop()
		if setProto.GetOptype() == ApbSetUpdate_ADD {
			tmpUpd = crdt.AddAll{Elems: crdt.ByteMatrixToElementArray(setProto.GetAdds())}
		} else {
			tmpUpd = crdt.RemoveAll{Elems: crdt.ByteMatrixToElementArray(setProto.GetRems())}
		}
	case CRDTType_ORMAP:
		mapProto := op.GetMapop()
		if adds := mapProto.GetUpdates(); len(adds) > 0 {
			updAdds := make(map[string]crdt.Element)
			for _, mapUpd := range adds {
				updAdds[string(mapUpd.GetKey().GetKey())] =
					crdt.Element(mapUpd.GetUpdate().GetRegop().GetValue())
			}
			tmpUpd = crdt.MapAddAll{Values: updAdds}
		} else {
			rems := mapProto.GetRemovedKeys()
			updRems := make([]string, len(rems))
			i := 0
			for _, mapUpd := range rems {
				updRems[i] = string(mapUpd.GetKey())
				i++
			}
			tmpUpd = crdt.MapRemoveAll{Keys: updRems}
		}
	case CRDTType_RRMAP:
		mapProto := op.GetMapop()
		if adds := mapProto.GetUpdates(); len(adds) > 0 {
			updAdds := make(map[string]crdt.UpdateArguments)
			for _, mapUpd := range adds {
				protoKey := mapUpd.GetKey()
				updAdds[string(protoKey.GetKey())] =
					*ConvertProtoUpdateToAntidote(mapUpd.GetUpdate(), protoKey.GetType())
			}
			tmpUpd = crdt.EmbMapUpdateAll{Upds: updAdds}
		} else {
			rems := mapProto.GetRemovedKeys()
			updRems := make([]string, len(rems))
			i := 0
			for _, mapUpd := range rems {
				updRems[i] = string(mapUpd.GetKey())
				i++
			}
			tmpUpd = crdt.MapRemoveAll{Keys: updRems}
		}

	case CRDTType_TOPK_RMV:
		topkProto := op.GetTopkrmvop()
		if adds := topkProto.GetAdds(); adds != nil {
			add := adds[0]
			tmpUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: add.GetPlayerId(), Score: add.GetScore()}}
		} else {
			rem := topkProto.GetRems()[0]
			tmpUpd = crdt.TopKRemove{Id: rem}
		}
	case CRDTType_AVG:
		avgProto := op.GetAvgop()
		tmpUpd = crdt.AddMultipleValue{SumValue: avgProto.GetValue(), NAdds: avgProto.GetNValues()}
	case CRDTType_MAXMIN:
		maxMinProto := op.GetMaxminop()
		if maxMinProto.GetIsMax() {
			tmpUpd = crdt.MaxAddValue{Value: maxMinProto.GetValue()}
		} else {
			tmpUpd = crdt.MinAddValue{Value: maxMinProto.GetValue()}
		}
	default:
		//TODO: Support other types and error case, and return error to client
		tools.CheckErr("Unsupported data type for update - we should warn the user about this one day.", nil)
	}

	return &tmpUpd
}

func convertAntidoteStatesToProto(objectStates []crdt.State) (protobufs []*ApbReadObjectResp) {
	protobufs = make([]*ApbReadObjectResp, len(objectStates))
	for i, state := range objectStates {
		protobufs[i] = convertAntidoteStateToProto(state)
	}
	return
}

func convertAntidoteStateToProto(objectState crdt.State) (protobuf *ApbReadObjectResp) {
	switch convertedState := objectState.(type) {
	case crdt.CounterState:
		protobuf = createCounterReadResp(convertedState.Value)
	case crdt.RegisterState:
		protobuf = createLWWRegisterReadResp(convertedState.Value)
	case crdt.SetAWValueState:
		protobuf = createSetReadResp(convertedState.Elems)
	case crdt.MapEntryState:
		protobuf = createMapReadResp(convertedState.Values)
	case crdt.EmbMapEntryState:
		protobuf = createRWMapReadResp(convertedState.States)
	case crdt.TopKValueState:
		protobuf = createTopkReadResp(convertedState.Scores)
	case crdt.AvgState:
		protobuf = createAvgReadResp(convertedState.Value)
	case crdt.MaxMinState:
		protobuf = createMaxMinReadResp(convertedState.Value)
	default:
		tools.CheckErr("Unsupported data type in convertAntidoteStatesToProto", nil)
	}
	return
}

func ConvertProtoObjectToAntidoteState(proto *ApbReadObjectResp, crdtType CRDTType) (state crdt.State) {
	switch crdtType {
	case CRDTType_COUNTER:
		state = crdt.CounterState{Value: proto.GetCounter().GetValue()}
	case CRDTType_LWWREG:
		state = crdt.RegisterState{Value: string(proto.GetReg().GetValue())}
	case CRDTType_ORSET:
		allObjsBytes := proto.GetSet().GetValue()
		setState := crdt.SetAWValueState{Elems: make([]crdt.Element, len(allObjsBytes))}
		//Convert byte[][] back to strings
		for i, objBytes := range allObjsBytes {
			setState.Elems[i] = crdt.Element(objBytes)
		}
		state = setState
	case CRDTType_ORMAP:
		entries := proto.GetMap().GetEntries()
		mapState := crdt.MapEntryState{Values: make(map[string]crdt.Element)}
		for _, entry := range entries {
			mapState.Values[string(entry.GetKey().GetKey())] = crdt.Element(entry.GetValue().GetReg().GetValue())
		}
		state = mapState
	case CRDTType_RRMAP:
		entries := proto.GetMap().GetEntries()
		mapState := crdt.EmbMapEntryState{States: make(map[string]crdt.State)}
		for _, entry := range entries {
			protoKey := entry.GetKey()
			mapState.States[string(protoKey.GetKey())] = ConvertProtoObjectToAntidoteState(entry.GetValue(), protoKey.GetType())
		}
		state = mapState
	case CRDTType_TOPK_RMV:
		scores := proto.GetTopk().GetValues()
		topKState := crdt.TopKValueState{Scores: make([]crdt.TopKScore, len(scores))}
		for i, pair := range scores {
			topKState.Scores[i] = crdt.TopKScore{Id: pair.GetPlayerId(), Score: pair.GetScore()}
		}
		state = topKState
	case CRDTType_AVG:
		state = crdt.AvgState{Value: proto.GetAvg().GetAvg()}
	case CRDTType_MAXMIN:
		state = crdt.MaxMinState{Value: proto.GetMaxmin().GetValue()}
	}
	return
}

func stringArrayToMapKeyArray(keys []string) (converted []*ApbMapKey) {
	converted = make([]*ApbMapKey, len(keys))
	crdtType := CRDTType_LWWREG
	for i, key := range keys {
		converted[i] = &ApbMapKey{Key: []byte(key), Type: &crdtType}
	}
	return
}

func mapEntriesToProto(entries map[string]crdt.Element) (converted []*ApbMapNestedUpdate) {
	converted = make([]*ApbMapNestedUpdate, len(entries))
	crdtType := CRDTType_LWWREG
	i := 0
	for key, elem := range entries {
		converted[i] = &ApbMapNestedUpdate{
			Key:    &ApbMapKey{Key: []byte(key), Type: &crdtType},
			Update: &ApbUpdateOperation{Regop: &ApbRegUpdate{Value: []byte(elem)}},
		}
		i++
	}
	return
}

func createMapNestedOps(upds map[string]crdt.UpdateArguments) (converted []*ApbMapNestedUpdate) {
	converted = make([]*ApbMapNestedUpdate, len(upds))
	i := 0
	for key, upd := range upds {
		crdtType := CRDTType(upd.GetCRDTType())
		converted[i] = &ApbMapNestedUpdate{
			Key:    &ApbMapKey{Key: []byte(key), Type: &crdtType},
			Update: createUpdateOperation(upd, crdtType),
		}
		i++
	}
	return
}

/***** REPLICATOR PROTOS ******/

/*
func createProtoReplicatePart(request *NewReplicatorRequest) (protobuf *ProtoReplicatePart) {
	return &ProtoReplicatePart{
		SenderID:     &request.SenderID,
		PartitionID:  &request.PartitionID,
		PartStableTs: &request.StableTs,
		Txns:         createProtoRemoteTxns(request.Txns),
	}
}
*/

/*
func createProtoRemoteTxns(txnsRequests []NewRemoteTxns) (protobufs []*ProtoRemoteTxns) {
	protobufs = make([]*ProtoRemoteTxns, len(txnsRequests))
	for i, req := range txnsRequests {
		protobufs[i] = &ProtoRemoteTxns{
			Timestamp: req.Timestamp.ToBytes(),
			Upds:      createProtoDownstreamUpds(&req),
		}
	}
	return protobufs
}
*/

func createProtoStableClock(replicaID int64, ts int64) (protobuf *ProtoStableClock) {
	return &ProtoStableClock{SenderID: &replicaID, ReplicaTs: &ts}
}

func createProtoReplicatePart(replicaID int64, partitionID int64, timestamp clocksi.Timestamp, upds []*UpdateObjectParams) (protobuf *ProtoReplicatePart) {
	return &ProtoReplicatePart{
		SenderID:    &replicaID,
		PartitionID: &partitionID,
		Txn: &ProtoRemoteTxn{
			Timestamp: timestamp.ToBytes(),
			Upds:      createProtoDownstreamUpds(upds),
		},
	}
}

func createProtoDownstreamUpds(upds []*UpdateObjectParams) (protobufs []*ProtoDownstreamUpd) {
	protobufs = make([]*ProtoDownstreamUpd, len(upds))
	for i, upd := range upds {
		protobufs[i] = &ProtoDownstreamUpd{
			KeyParams: createBoundObject(upd.Key, upd.CrdtType, upd.Bucket),
			Op:        createProtoOpDownstream(upd.UpdateArgs),
		}
	}
	return protobufs
}

func createProtoOpDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoOpDownstream) {
	protobuf = &ProtoOpDownstream{}
	switch CRDTType((*(upd)).GetCRDTType()) {
	case CRDTType_COUNTER:
		protobuf.CounterOp = createProtoCounterDownstream(upd)
	case CRDTType_LWWREG:
		protobuf.LwwregOp = createProtoLWWRegisterDownstream(upd)
	case CRDTType_ORSET:
		protobuf.SetOp = createProtoSetDownstream(upd)
	case CRDTType_ORMAP:
		protobuf.OrmapOp = createProtoMapDownstream(upd)
	case CRDTType_RRMAP:
		protobuf.RwembmapOp = createProtoEmbMapDownstream(upd)
	case CRDTType_TOPK_RMV:
		protobuf.TopkrmvOp = createProtoTopkRmvDownstream(upd)
	case CRDTType_AVG:
		protobuf.AvgOp = createProtoAvgDownstream(upd)
	case CRDTType_MAXMIN:
		protobuf.MaxminOp = createProtoMaxMinDownstream(upd)
	}
	return
}

func createProtoCounterDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoCounterDownstream) {
	switch convertedUpd := (*upd).(type) {
	case crdt.Increment:
		return &ProtoCounterDownstream{IsInc: proto.Bool(true), Change: &convertedUpd.Change}
	case crdt.Decrement:
		return &ProtoCounterDownstream{IsInc: proto.Bool(false), Change: &convertedUpd.Change}
	}
	return nil
}

func createProtoLWWRegisterDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoLWWRegisterDownstream) {
	convertedUpd := (*upd).(crdt.DownstreamSetValue)
	return &ProtoLWWRegisterDownstream{Value: []byte((convertedUpd.NewValue).(string)), Ts: &convertedUpd.Ts, ReplicaID: &convertedUpd.ReplicaID}
}

func createProtoSetDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoSetDownstream) {
	switch convertedUpd := (*upd).(type) {
	case crdt.DownstreamAddAll:
		addProto := &ProtoSetDownstream{Adds: make([]*ProtoValueUnique, len(convertedUpd.Elems))}
		i := 0
		for value, unique := range convertedUpd.Elems {
			intUnique := uint64(unique)
			addProto.Adds[i] = &ProtoValueUnique{Value: []byte(value), Unique: &intUnique}
			i++
		}
		protobuf = addProto
	case crdt.DownstreamRemoveAll:
		remProto := &ProtoSetDownstream{Rems: make([]*ProtoValueUniques, len(convertedUpd.Elems))}
		i := 0
		for value, uniques := range convertedUpd.Elems {
			//TODO: Probably pass this uniquesInt thing to commonTools
			tools.FancyInfoPrint(tools.PROTOLIB_PRINT, -1, "Encoding uniques for remove. ", value, uniques)
			uniquesInts := crdt.UniqueSetToUInt64Array(uniques)
			remProto.Rems[i] = &ProtoValueUniques{Value: []byte(value), Uniques: uniquesInts}
			tools.FancyInfoPrint(tools.PROTOLIB_PRINT, -1, "Encoded uniques (ints).", value, uniquesInts)
			i++
		}
		protobuf = remProto
	}
	return
}

func createProtoMapDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoORMapDownstream) {
	switch convUpd := (*upd).(type) {
	case crdt.DownstreamORMapAddAll:
		protobuf = &ProtoORMapDownstream{
			Adds: make([]*ProtoKeyValueUnique, len(convUpd.Adds)), Rems: createProtoMapRemove(convUpd.Rems),
		}
		i := 0
		for key, pair := range convUpd.Adds {
			protobuf.Adds[i] = &ProtoKeyValueUnique{
				Key: []byte(key), Element: []byte(pair.Element), Unique: proto.Uint64(uint64(pair.Unique)),
			}
			i++
		}
	case crdt.DownstreamORMapRemoveAll:
		protobuf = &ProtoORMapDownstream{Rems: createProtoMapRemove(convUpd.Rems)}
	}
	return
}

func createProtoMapRemove(rems map[string]map[crdt.Element]crdt.UniqueSet) (protos []*ProtoORMapRemove) {
	protos = make([]*ProtoORMapRemove, len(rems))
	i, j := 0, 0
	for key, elems := range rems {
		protoElems := make([]*ProtoValueUniques, len(elems))
		for elem, uniques := range elems {
			protoElems[j] = &ProtoValueUniques{Value: []byte(elem), Uniques: crdt.UniqueSetToUInt64Array(uniques)}
			j++
		}
		protos[i] = &ProtoORMapRemove{Key: []byte(key), Elems: protoElems}
	}
	return
}

func createProtoEmbMapDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoRWEmbMapDownstream) {
	i, j := 0, 0
	switch convUpd := (*upd).(type) {
	case crdt.DownstreamRWEmbMapUpdateAll:
		protobuf = &ProtoRWEmbMapDownstream{Adds: &ProtoRWEmbMapUpdates{
			Upds:      make([]*ProtoEmbMapUpd, len(convUpd.Upds)),
			Vc:        make([]*ProtoStableClock, len(convUpd.RmvEntries)),
			ReplicaID: proto.Int64(convUpd.ReplicaID),
		}}
		for key, upd := range convUpd.Upds {
			//TODO: Most of the methods here should be dealing with DownstreamArgs instead of UpdateArgs tbh
			updArgs := upd.(crdt.UpdateArguments)
			crdtType := CRDTType(updArgs.GetCRDTType())
			protobuf.Adds.Upds[i] = &ProtoEmbMapUpd{Key: []byte(key), Upd: createProtoOpDownstream(&updArgs), Type: &crdtType}
			i += 1
		}
		for key, clk := range convUpd.RmvEntries {
			protobuf.Adds.Vc[j] = &ProtoStableClock{SenderID: proto.Int64(key), ReplicaTs: proto.Int64(clk)}
			j += 1
		}
	case crdt.DownstreamRWEmbMapRemoveAll:
		protobuf = &ProtoRWEmbMapDownstream{Rems: &ProtoRWEmbMapRemoves{
			Keys: convUpd.Rems, ReplicaID: proto.Int64(convUpd.ReplicaID), Ts: proto.Int64(convUpd.Ts),
		}}
	}
	return
}

func createProtoTopkRmvDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoTopKRmvDownstream) {
	switch convUpd := (*upd).(type) {
	case crdt.DownstreamTopKAdd:
		protobuf = &ProtoTopKRmvDownstream{Adds: make([]*ProtoTopKElement, 1)}
		protobuf.Adds[0] = &ProtoTopKElement{Id: proto.Int32(convUpd.Id), Score: proto.Int32(convUpd.Score),
			Ts: proto.Int64(convUpd.Ts), ReplicaID: proto.Int64(convUpd.ReplicaID)}
	case crdt.DownstreamTopKRemove:
		protobuf = &ProtoTopKRmvDownstream{Rems: make([]*ProtoTopKIdVc, 1)}
		protobuf.Rems[0] = &ProtoTopKIdVc{Id: proto.Int32(convUpd.Id), Vc: convUpd.Vc.ToBytes()}
	}
	return
}

func createProtoAvgDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoAvgDownstream) {
	switch convUpd := (*upd).(type) {
	case crdt.AddValue:
		protobuf = &ProtoAvgDownstream{SumValue: proto.Int64(convUpd.Value), NAdds: proto.Int64(1)}
	case crdt.AddMultipleValue:
		protobuf = &ProtoAvgDownstream{SumValue: proto.Int64(convUpd.SumValue), NAdds: proto.Int64(convUpd.NAdds)}
	}
	return
}

func createProtoMaxMinDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoMaxMinDownstream) {
	switch convUpd := (*upd).(type) {
	case crdt.MaxAddValue:
		protobuf = &ProtoMaxMinDownstream{Max: &ProtoMaxDownstream{Value: proto.Int64(convUpd.Value)}}
	case crdt.MinAddValue:
		protobuf = &ProtoMaxMinDownstream{Min: &ProtoMinDownstream{Value: proto.Int64(convUpd.Value)}}
	}
	return
}

func protoToStableClock(protobuf *ProtoStableClock) (stableClk *StableClock) {
	return &StableClock{SenderID: protobuf.GetSenderID(), Ts: protobuf.GetReplicaTs()}
}

func protoToReplicatorRequest(protobuf *ProtoReplicatePart) (request *NewReplicatorRequest) {
	return &NewReplicatorRequest{
		PartitionID: protobuf.GetPartitionID(),
		SenderID:    protobuf.GetSenderID(),
		Timestamp:   clocksi.ClockSiTimestamp{}.FromBytes(protobuf.Txn.Timestamp),
		Upds:        protoToDownstreamUpds(protobuf.Txn.Upds),
	}
}

func protoToDownstreamUpds(protobufs []*ProtoDownstreamUpd) (upds []*UpdateObjectParams) {
	upds = make([]*UpdateObjectParams, len(protobufs))
	for i, proto := range protobufs {
		keyProto := proto.GetKeyParams()
		upd := &UpdateObjectParams{
			KeyParams: CreateKeyParams(string(keyProto.GetKey()), keyProto.GetType(), string(keyProto.GetBucket())),
		}
		tmpArgs := protoToUpdateArgs(upd.CrdtType, proto.GetOp())
		upd.UpdateArgs = &tmpArgs
		upds[i] = upd
	}
	return upds
}

func protoToUpdateArgs(crdtType CRDTType, proto *ProtoOpDownstream) (args crdt.UpdateArguments) {
	switch crdtType {
	case CRDTType_COUNTER:
		args = protoToCounterDownstream(proto.GetCounterOp())
	case CRDTType_LWWREG:
		args = protoToLWWRegisterDownstream(proto.GetLwwregOp())
	case CRDTType_ORSET:
		args = protoToSetDownstream(proto.GetSetOp())
	case CRDTType_ORMAP:
		args = protoToMapDownstream(proto.GetOrmapOp())
	case CRDTType_RRMAP:
		args = protoToEmbMapDownstream(proto.GetRwembmapOp())
	case CRDTType_TOPK_RMV:
		args = protoToTopkrmvDownstream(proto.GetTopkrmvOp())
	case CRDTType_AVG:
		args = protoToAvgDownstream(proto.GetAvgOp())
	case CRDTType_MAXMIN:
		args = protoToMaxMinDownstream(proto.GetMaxminOp())
	}
	return
}

func protoToCounterDownstream(protobuf *ProtoCounterDownstream) (args crdt.UpdateArguments) {
	if protobuf.GetIsInc() {
		return crdt.Increment{Change: protobuf.GetChange()}
	} else {
		return crdt.Decrement{Change: protobuf.GetChange()}
	}
}

func protoToLWWRegisterDownstream(protobuf *ProtoLWWRegisterDownstream) (args crdt.UpdateArguments) {
	return crdt.DownstreamSetValue{NewValue: string(protobuf.GetValue()), Ts: protobuf.GetTs(), ReplicaID: protobuf.GetReplicaID()}
}

func protoToSetDownstream(protobuf *ProtoSetDownstream) (args crdt.UpdateArguments) {
	if adds := protobuf.GetAdds(); adds != nil {
		tools.FancyInfoPrint(tools.PROTOLIB_PRINT, -1, "Decoding set add. Rems:", protobuf.GetRems())
		elems := make(map[crdt.Element]crdt.Unique)
		for _, pairProto := range adds {
			elems[crdt.Element(pairProto.GetValue())] = crdt.Unique(pairProto.GetUnique())
		}
		return crdt.DownstreamAddAll{Elems: elems}
	} else {
		tools.FancyWarnPrint(tools.PROTOLIB_PRINT, -1, "Decoding set remove. Rems:", protobuf.GetRems())
		elems := make(map[crdt.Element]crdt.UniqueSet)
		for _, pairProto := range protobuf.GetRems() {
			elems[crdt.Element(pairProto.GetValue())] = crdt.UInt64ArrayToUniqueSet(pairProto.GetUniques())
		}
		return crdt.DownstreamRemoveAll{Elems: elems}
	}
}

func protoToMapDownstream(protobuf *ProtoORMapDownstream) (args crdt.UpdateArguments) {
	rems := protobuf.GetRems()
	//Both DownstreamAdd and DownstreamRem have rems

	crdtRems := make(map[string]map[crdt.Element]crdt.UniqueSet)
	for _, remProto := range rems {
		elemMap := make(map[crdt.Element]crdt.UniqueSet)
		for _, elemProto := range remProto.GetElems() {
			elemMap[crdt.Element(elemProto.GetValue())] = crdt.UInt64ArrayToUniqueSet(elemProto.GetUniques())
		}
		crdtRems[string(remProto.GetKey())] = elemMap
	}

	if adds := protobuf.GetAdds(); adds != nil {
		//Add
		crdtAdds := make(map[string]crdt.UniqueElemPair)
		for _, addProto := range adds {
			crdtAdds[string(addProto.GetKey())] = crdt.UniqueElemPair{
				Element: crdt.Element(addProto.GetElement()),
				Unique:  crdt.Unique(addProto.GetUnique()),
			}
		}
		return crdt.DownstreamORMapAddAll{Adds: crdtAdds, Rems: crdtRems}
	}
	//Rem
	return crdt.DownstreamORMapRemoveAll{Rems: crdtRems}
}

func protoToEmbMapDownstream(protobuf *ProtoRWEmbMapDownstream) (args crdt.UpdateArguments) {
	if adds := protobuf.GetAdds(); adds != nil {
		upds := adds.GetUpds()
		clk := adds.GetVc()
		updsMap := make(map[string]crdt.DownstreamArguments)
		clkMap := make(map[int64]int64)

		for _, upd := range upds {
			//TODO: Update this to not use the conversion
			updsMap[string(upd.GetKey())] = protoToUpdateArgs(upd.GetType(), upd.GetUpd()).(crdt.DownstreamArguments)
		}
		for _, protoEntry := range clk {
			clkMap[protoEntry.GetSenderID()] = protoEntry.GetReplicaTs()
		}

		args = crdt.DownstreamRWEmbMapUpdateAll{Upds: updsMap, RmvEntries: clkMap, ReplicaID: adds.GetReplicaID()}
	} else {
		rems := protobuf.GetRems()
		args = crdt.DownstreamRWEmbMapRemoveAll{Rems: rems.GetKeys(), Ts: rems.GetTs(), ReplicaID: rems.GetReplicaID()}
	}
	return
}

func protoToTopkrmvDownstream(protobuf *ProtoTopKRmvDownstream) (args crdt.UpdateArguments) {
	if adds := protobuf.GetAdds(); adds != nil {
		//For now it's only 1 op
		add := adds[0]
		return crdt.DownstreamTopKAdd{TopKElement: crdt.TopKElement{Id: *add.Id, Score: *add.Score, Ts: *add.Ts, ReplicaID: *add.ReplicaID}}
	} else {
		//For now it's only 1 op
		rem := protobuf.GetRems()[0]
		return crdt.DownstreamTopKRemove{Id: *rem.Id, Vc: clocksi.ClockSiTimestamp{}.FromBytes(rem.Vc)}
	}
}

func protoToAvgDownstream(protobuf *ProtoAvgDownstream) (args crdt.UpdateArguments) {
	return crdt.AddMultipleValue{SumValue: protobuf.GetSumValue(), NAdds: protobuf.GetNAdds()}
}

func protoToMaxMinDownstream(protobuf *ProtoMaxMinDownstream) (args crdt.UpdateArguments) {
	if max := protobuf.GetMax(); max != nil {
		return crdt.MaxAddValue{Value: max.GetValue()}
	}
	return crdt.MinAddValue{Value: protobuf.GetMin().GetValue()}
}
