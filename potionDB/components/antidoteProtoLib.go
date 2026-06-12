package components

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	fmt "fmt"
	"io"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"potionDB/potionDB/utilities"
	"potionDB/shared/shared"

	//pb "github.com/golang/protobuf/proto"
	"github.com/AndreRijo/go-tools/src/tools"
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
	utilities.CheckErr("Sending protobuf err:", err)
	//fmt.Printf("Protobuf code %d sent succesfully!\n", code)
}

func SendProtoS2SDebug(code byte, protobf *proto.S2SWrapper, writer io.Writer) {
	toSend, err := pb.Marshal(protobf)
	if err != nil {
		panic(fmt.Sprintf("Marshal err: %s. ClientID: %d. S2S inner msg type: %d. Protobf: %v", err, protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	protoSize := len(toSend)
	if protoSize <= 0 {
		panic(fmt.Sprintf("Invalid proto size after Marshal. Size: %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", protoSize, protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	buffer := make([]byte, protoSize+5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = code
	copy(buffer[5:], toSend)
	if code != S2S {
		panic(fmt.Sprintf("Error, just before sending, unexpected code in SendProtoS2SDebug: got %d, expected %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", code, S2S, protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	if protoSize > 1000 {
		if protoSize > 5000 {
			panic(fmt.Sprintf("Error, just before sending, unexpectedly large proto size in SendProtoS2SDebug: got %d. Code: %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", protoSize, code, protobf.GetClientID(), protobf.GetMsgID(), protobf))
		}
		panic(fmt.Sprintf("Warning: Large proto size in SendProtoS2SDebug: got %d. Code: %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", protoSize, code, protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	nWritten, err := writer.Write(buffer)
	if code != S2S {
		panic(fmt.Sprintf("Error, unexpected code in SendProtoS2SDebug: got %d, expected %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", code, S2S, protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	if err != nil {
		panic(fmt.Sprintf("Error writing buffer: %s. Written: %d, expected: %d. Msg size (-5 than expected): %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", err, nWritten, len(buffer), len(toSend), protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	if nWritten != len(buffer) {
		panic(fmt.Sprintf("Did not write full buffer. Written: %d, expected: %d. Msg size (-5 than expected): %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", nWritten, len(buffer), len(toSend), protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	if protobf.GetMsgID() != proto.WrapperType_STATIC_UPDATE {
		panic(fmt.Sprintf("Error, wrapper inner type is %d instead of %d.\n", protobf.GetMsgID(), proto.WrapperType_STATIC_UPDATE))
	}
	newPb := &proto.S2SWrapper{}
	err = pb.Unmarshal(buffer[5:], newPb)
	if err != nil {
		fmt.Printf("[SPS2SDebug]Error during unmarshalling. Sent S2S req, code %d, writtenSize %d, bufSize %d, nWritten %d, clientID %d. Unmarshall msg: %+v.\n",
			buffer[4], binary.BigEndian.Uint32(buffer[0:4]), len(buffer), nWritten, protobf.GetClientID(), newPb)
		panic(fmt.Sprintf("Error unmarshalling sent proto for debug check: %s. ClientID: %d. S2S inner msg type: %d. Protobf: %+v", err, protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	//	buffer[4], binary.BigEndian.Uint32(buffer[0:4]), len(buffer), nWritten, protobf.GetClientID(), newPb)
}

func SendProtoS2SReplyDebug(code byte, protobf *proto.S2SWrapperReply, writer io.Writer) {
	toSend, err := pb.Marshal(protobf)
	if err != nil {
		panic(fmt.Sprintf("Marshal err: %s. ClientID: %d. S2S inner msg type: %d. Protobf: %v", err, protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	protoSize := len(toSend)
	if protoSize <= 0 {
		panic(fmt.Sprintf("Invalid proto size after Marshal. Size: %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", protoSize, protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	buffer := make([]byte, protoSize+5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = code
	copy(buffer[5:], toSend)
	nWritten, err := writer.Write(buffer)
	if err != nil {
		panic(fmt.Sprintf("Error writing buffer: %s. Written: %d, expected: %d. Msg size (-5 than expected): %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", err, nWritten, len(buffer), len(toSend), protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
	if nWritten != len(buffer) {
		panic(fmt.Sprintf("Did not write full buffer. Written: %d, expected: %d. Msg size (-5 than expected): %d. ClientID: %d. S2S inner msg type: %d. Protobf: %v", nWritten, len(buffer), len(toSend), protobf.GetClientID(), protobf.GetMsgID(), protobf))
	}
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
	utilities.CheckErr("Marshal err", err)
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
	utilities.CheckErr("Marshal err", err)
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

func SendProtoReusableBufVT(code byte, protobf pb.Message, writer io.Writer, buf []byte) (err error, newBuf []byte) {
	vtProto := protobf.(proto.VTMessage)
	size := vtProto.SizeVT() + 5 //+5: 4 bytes for size, 1 byte for code
	if len(buf) < size {
		buf = make([]byte, size+size/10) //A bit extra space in case the next message is only slightly bigger.
	}
	binary.BigEndian.PutUint32(buf[0:4], uint32(size-4)) //We don't include the 4 bytes for size here, but we do include the 1 byte for code.
	buf[4] = code
	vtProto.MarshalToSizedBufferVT(buf[5:size])
	/*fmt.Printf("[SendProtoReusableBufVT]Needed size: %d. Buf cap: %d. Size written (+1 of marshall): %d. Code: %d. Marshall written: %d.\n",
		size, cap(buf), binary.BigEndian.Uint32(buf[0:4]), code, nWritten)
	if err != nil {
		fmt.Printf("[SendProtoReusableBufVT]Error marshalling proto: %s. Code: %d. Size needed: %d. Buf cap: %d.\n", err, code, size, cap(buf))
	}*/
	_, err = writer.Write(buf[:size])
	return err, buf[:cap(buf)] //Make full buffer available.
}

// Similar to SendProtoNoCheck, but it receives a reusable buffer to avoid allocations and reduce GC pressure.
// Pre: buf must not be nil and must have at least 6 bytes of capacity. Recommended initial size is 1KB (1024), as that should accomodate most simple protobuf messages.
func SendProtoReusableBuf(code byte, protobf pb.Message, writer io.Writer, buf []byte) (err error, newBuf []byte) {
	/*marOpt := pb.MarshalOptions{UseCachedSize: true}
	size := marOpt.Size(protobf) + 5 //+5: 4 bytes for size, 1 byte for code
	if cap(buf) < size {
		buf = make([]byte, size+size/10) //A bit extra space in case the next message is only slightly bigger.
	}
	newBuf, err = marOpt.MarshalAppend(buf[5:5], protobf) //5:5 - we give a buffer with len of 0, that will start at pos 5 (so that MarshalAppend won't override the 5 bytes header).
	if cap(newBuf) > cap(buf) {
		panic(fmt.Sprintf("Unexpected buffer reallocation. Old cap: %d, new cap: %d. This should not happen as we check the capacity before marshaling. Protobf: %v", cap(buf), cap(newBuf), protobf))
	}
	utilities.CheckErr("Marshal err", err)
	binary.BigEndian.PutUint32(buf[0:4], uint32(size-4)) //We don't include the 4 bytes for size here, but we do include the 1 byte for code.
	buf[4] = code
	_, err = writer.Write(buf[:size])
	return buf, err
	*/
	//fmt.Printf("[SendProtoReusableBuf]Marshalling.\n")
	newBuf, err = pb.MarshalOptions{}.MarshalAppend(buf[:5], protobf) //:5 - Ensures Marshall will start writing only after the first 5 bytes. If a resize happens, those 5 first bytes will be copied over.
	//fmt.Printf("[SendProtoReusableBuf]Finished marshalling.\n")
	utilities.CheckErr("Marshal err", err)
	binary.BigEndian.PutUint32(newBuf[0:4], uint32(len(newBuf)-4)) //We don't include the 4 bytes for size here, but we do include the 1 byte for code.
	newBuf[4] = code
	//fmt.Printf("[SendProtoReusableBuf]Sending %d bytes. Code %d, size in header: %d.\n", len(newBuf), code, binary.BigEndian.Uint32(newBuf[0:4]))
	_, err = writer.Write(newBuf)
	//fmt.Printf("[SendProtoReusableBuf]Finished sending %d bytes.\n", len(newBuf))
	return err, newBuf[:cap(newBuf)] //Make full buffer available.
}

func SendProtoMultiClient(code byte, client uint16, protobf pb.Message, writer io.Writer) {
	err := SendProtoMultiClientNoCheck(code, client, protobf, writer)
	utilities.CheckErr("Sending multi-client protobuf err:", err)
}

func SendProtoMultiClientNoCheck(code byte, client uint16, protobf pb.Message, writer io.Writer) (err error) {
	toSend, err := pb.Marshal(protobf)
	utilities.CheckErr("Marshal err", err)
	protoSize := len(toSend)
	buffer := make([]byte, protoSize+7)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+3))
	buffer[4] = code
	binary.BigEndian.PutUint16(buffer[5:7], client)
	copy(buffer[7:], toSend)
	_, err = writer.Write(buffer)
	return err
}

func SendProtoMultiClientNoCheckReusableBuf(code byte, client uint16, protobf pb.Message, writer io.Writer, buf []byte) (err error, newBuf []byte) {
	vtProto := protobf.(proto.VTMessage)
	size := vtProto.SizeVT() + 7 //+7: 4 bytes for size, 1 byte for code, 2 bytes for client ID
	if len(buf) < size {
		buf = make([]byte, size+size/10) //A bit extra space in case the next message is only slightly bigger.
	}
	binary.BigEndian.PutUint32(buf[0:4], uint32(size-4)) //We don't include the 4 bytes for size here, but we do include the 1 byte for code and 2 bytes for client ID.
	buf[4] = code
	binary.BigEndian.PutUint16(buf[5:7], client)
	vtProto.MarshalToSizedBufferVT(buf[7:size])
	_, err = writer.Write(buf[:size])
	return err, buf[:cap(buf)] //Make full buffer available.
}

func ReceiveProtoReusableBufferVTClientBuf(in io.Reader, buf []byte, clientBuf *ClientBuffers) (msgType byte, protobuf pb.Message, err error, newBuf []byte) {
	var msgData []byte
	msgType, newBuf, msgData, err = readProtoFromNetworkReusableBuffer(in, buf)
	if err != nil {
		if err != io.EOF {
			fmt.Printf("[WARNING]Returning error on ReceiveProtoReusableBuffer. MsgType: %v, msgBuf: %v, err: %s\n", msgType, newBuf, err)
		}
		return
	}
	protobuf = unmarshallProtoVTClientBuf(msgType, msgData, clientBuf.PbBuffers)
	return
}

func ReceiveProtoReusableBufferVT(in io.Reader, buf []byte) (msgType byte, protobuf pb.Message, err error, newBuf []byte) {
	var msgData []byte
	msgType, newBuf, msgData, err = readProtoFromNetworkReusableBuffer(in, buf)
	if err != nil {
		//fmt.Printf("[WARNING]Returning error on ReceiveProtoReusableBuffer. MsgType: %v, msgBuf: %v, err: %s\n", msgType, msgBuf, err)
		if err != io.EOF {
			fmt.Printf("[WARNING]Returning error on ReceiveProtoReusableBuffer. MsgType: %v, msgLength: %d, msgBuf: %v, err: %s\n", msgType, len(msgData), msgData, err)
		}
		//debug.PrintStack()
		return
	}
	protobuf = unmarshallProtoVT(msgType, msgData)
	return
}

func ReceiveProtoVT(in io.Reader) (msgType byte, protobuf pb.Message, err error) {
	msgType, msgBuf, err := readProtoFromNetwork(in)

	if err != nil {
		if err != io.EOF {
			fmt.Printf("[WARNING]Returning error on ReceiveProto. MsgType: %v, msgBuf: %v, err: %s\n", msgType, msgBuf, err)
		}
		return
	}
	if !isValidMsgType(msgType) {
		panic(fmt.Sprintf("[ERROR]Invalid msg type received: %d. Number of data bytes received: %d\n", msgType, len(msgBuf)))
	}
	protobuf = unmarshallProtoVT(msgType, msgBuf)
	return
}

// Similar to ReceiveProto, but it receives a reusable buffer to avoid allocations and reduce GC pressure.
// Pre: buf must not be nil and must have at least 6 bytes of capacity. Recommended initial size is 1KB (1024), as that should accomodate most simple protobuf messages.
func ReceiveProtoReusableBuffer(in io.Reader, buf []byte) (msgType byte, protobuf pb.Message, err error, newBuf []byte) {
	var msgData []byte
	msgType, newBuf, msgData, err = readProtoFromNetworkReusableBuffer(in, buf)
	if err != nil {
		//fmt.Printf("[WARNING]Returning error on ReceiveProtoReusableBuffer. MsgType: %v, msgBuf: %v, err: %s\n", msgType, msgBuf, err)
		if err != io.EOF {
			fmt.Printf("[WARNING]Returning error on ReceiveProtoReusableBuffer. MsgType: %v, msgBuf: %v, err: %s\n", msgType, newBuf, err)
		}
		//debug.PrintStack()
		return
	}
	if !isValidMsgType(msgType) {
		panic(fmt.Sprintf("[ERROR]Invalid msg type received: %d. Number of data bytes received: %d\n", msgType, len(msgData)))
	}
	protobuf = unmarshallProto(msgType, msgData)
	return
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
	//utilities.CheckErr("Receiving proto err:", err)
	//tsStart := time.Now().UnixNano()
	//fmt.Printf("[ReceiveProto]Unmarshalling proto.\n")
	protobuf = unmarshallProto(msgType, msgBuf)
	//fmt.Printf("[ReceiveProto]Finished unmarshalling proto.\n")
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
			//fmt.Printf("[WARNING]Error reading header in readProtoFromNetwork. Read so far: %d. Err: %s\n", nRead, err)
			return
		}
		nRead += n
	}
	msgSize := (int)(binary.BigEndian.Uint32(sizeBuf[:4])) - 1 //Disregard msgType byte.
	msgType = sizeBuf[4]
	/*if !isValidMsgType(msgType) {
		panic(fmt.Sprintf("[ERROR]Invalid msg type received: %d. Size received: %d.\n", msgType, msgSize))
	}
	if msgSize <= 0 {
		fmt.Printf("[ERROR]Invalid size received in readProtoFromNetwork: %d. Msg type %d. What even happened here?\n", msgSize, sizeBuf[4])
		panic(fmt.Sprintf("[ERROR]Invalid size received in readProtoFromNetwork: %d. Msg type %d. What even happened here?", msgSize, sizeBuf[4]))
	}*/
	msgBuf := make([]byte, msgSize)
	for nRead := 0; nRead < msgSize; {
		n, err = in.Read(msgBuf[nRead:])
		if err != nil {
			return
		}
		nRead += n
		//fmt.Printf("[ReadProtoFromNetwork]Received %d bytes out of %d, not counting with header. MsgSize (w/o header but with msgType): %d. MsgType: %d.\n", nRead, msgSize-1, msgSize, msgType)
	}
	msgData = msgBuf
	return
}

func readProtoFromNetworkReusableBuffer(in io.Reader, buf []byte) (msgType byte, newBuf, msgData []byte, err error) {
	n := 0
	//buf = make([]byte, 2000) //TODO: Remove.
	for nRead := 0; nRead < 5; {
		n, err = in.Read(buf[nRead:5])
		if err != nil {
			//mt.Printf("[ERROR]Error reading header in readProtoFromNetworkReusableBuffer. Read so far: %d. Err: %s\n", nRead, err)
			return
		}
		nRead += n
	}
	msgSize := (int)(binary.BigEndian.Uint32(buf[:4])) - 1 //We don't need to consider the 1 byte used by msgType anymore.
	msgType = buf[4]
	/*if !isValidMsgType(msgType) {
		//panic(fmt.Sprintf("[ERROR]Invalid msg type received: %d. Size received: %d.\n", msgType, msgSize))
		fmt.Printf("[ERROR]Invalid msg type received: %d. Size received: %d. Will continue (even though it will block)\n", msgType, msgSize)
	}
	if msgSize <= 0 {
		if msgSize == 0 {
			fmt.Printf("[WARNING]msgSize is 0 (after -1), so content is empty. Msg type %d. May be OK for msgs such as ApbServerConn.\n", buf[4])
			return msgType, buf, buf[:msgSize], err
		} else {
			fmt.Printf("[ERROR]Invalid size received in readProtoFromNetworkReusableBuffer: %d. Msg type %d. What even happened here?\n", msgSize, buf[4])
			panic(fmt.Sprintf("[ERROR]Invalid size received in readProtoFromNetworkReusableBuffer: %d. Msg type %d. What even happened here?", msgSize, buf[4]))
		}
	}*/
	if msgSize > cap(buf) {
		buf = make([]byte, msgSize+msgSize/10) //A bit extra space in case the next message is only slightly bigger.
	} /* else if msgSize > len(buf) {
		panic(fmt.Sprintf("[ERROR]Buffer received has enough capacity but not length. This is a bug. Len, cap: %d, %d. Msg size: %d. Msg type %d.", len(buf), cap(buf), msgSize, buf[4]))
	}*/
	nRead := 0
	msgData = buf[:msgSize] //This is important to ensure we don't read more bytes than intended (in case the sender already sent more messages)
	for nRead < msgSize {   //We'll override msgSize and msgType's bytes, but that's ok as we no longer need them.
		n, err = in.Read(msgData[nRead:])
		if err != nil {
			//fmt.Printf("[ERROR]Error while reading msg contents in readProtoFromNetworkReusableBuffer. Read so far: %d. Expected to read: %d. MsgType: %d. Err: %s\n", nRead, msgSize, msgType, err)
			return msgType, buf, msgData[:nRead+n], err
		}
		nRead += n
	}
	/*if nRead > msgSize {
		panic(fmt.Sprintf("[ERROR]Read more bytes than expected in readProtoFromNetworkReusableBuffer. Read: %d. Expected: %d. MsgType: %d.", nRead, msgSize, msgType))
	}*/
	return msgType, buf, msgData, err
}

func isValidMsgType(msgType byte) bool {
	switch msgType {
	case ConnectReplica, ReadObjs, Read, StaticRead, UpdateObjs, StartTrans, AbortTrans, CommitTrans, StaticUpdateObjs, StaticReadObjs, ResetServer, NewTrigger, GetTriggers, ServerConn, S2S, ServerConnReplicaID, SQLString, SQLTyped, MultiConnect,
		ConnectReplicaReply, OpReply, StartTransReply, ReadObjsReply, CommitTransReply, StaticReadObjsReply, ResetServerReply, NewTriggerReply, GetTriggersReply, S2SReply, MultiConnectReply,
		ErrorReply:
		return true
	default:
		return false
	}
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

func ReceiveProtoMultiClientReusableBuf(in io.Reader, buf []byte) (msgType byte, client uint16, protobuf pb.Message, err error, newBuf []byte) {
	var msgData []byte
	msgType, client, newBuf, msgData, err = readProtoFromNetworkMultiClientReusableBuf(in, buf)

	if err != nil {
		if err != io.EOF {
			fmt.Printf("[WARNING]Returning error on ReceiveProtoMultiClient. MsgType: %v, protobuf: %v, err: %s\n", msgType, protobuf, err)
		}
		return
	}

	//protobuf = unmarshallProto(msgType, newBuf)
	protobuf = unmarshallProtoVT(msgType, msgData)
	return
}

func readProtoFromNetworkMultiClientReusableBuf(in io.Reader, buf []byte) (msgType byte, client uint16, newBuf, msgData []byte, err error) {
	n := 0
	for nRead := 0; nRead < 7; {
		n, err = in.Read(buf[nRead:7])
		if err != nil {
			return
		}
		nRead += n
	}
	msgSize := (int)(binary.BigEndian.Uint32(buf[:4])) - 3 //Don't consider the, resp, 1 and 2 bytes used by msgType and clientID.
	msgType = buf[4]
	client = binary.BigEndian.Uint16(buf[5:7])
	if msgSize > cap(buf) {
		buf = make([]byte, msgSize+msgSize/10) //A bit extra space in case the next message is only slightly bigger.
	}
	for nRead := 0; nRead < msgSize; {
		n, err = in.Read(buf[nRead:])
		if err != nil {
			return
		}
		nRead += n
	}
	return msgType, client, buf, buf[:msgSize], err
}

/*****PUBLIC CONVERSION*****/

/*****REQUEST PROTOS*****/

var rw, rb uint32 = 0, 0

// Note: timestamp can be nil.
func CreateStartTransaction(timestamp []byte) (protoBuf *proto.ApbStartTransaction) {
	transProps := &proto.ApbTxnProperties{
		ReadWrite: &rw,
		RedBlue:   &rb,
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

func CreateServerConn(replicaID uint16) (protobuf *proto.ApbServerConn) {
	return &proto.ApbServerConn{ReplicaID: pb.Int32(int32(replicaID))}
}

func CreateServerConnReplicaID(replicaID uint16, buckets []string, serverIP string) *proto.ApbServerConnReplicaID {
	return &proto.ApbServerConnReplicaID{ReplicaID: pb.Int32(int32(replicaID)), MyBuckets: buckets, MyIP: &serverIP}
}

func CreateMultiClientConn(nClients int) (protobuf *proto.ApbMultiClientConnect) {
	return &proto.ApbMultiClientConnect{NClients: pb.Uint32(uint32(nClients))}
}

/*****REPLY/RESP PROTOS*****/

func CreateStartTransactionResp(txnId TransactionId, ts clocksi.Timestamp) (protobuf *proto.ApbStartTransactionResp) {
	protobuf = &proto.ApbStartTransactionResp{
		Success:               shared.TRUE_POINTER,
		TransactionDescriptor: createTxnDescriptorBytes(txnId, ts),
	}
	return
}

func CreateCommitOkResp(txnId TransactionId, ts clocksi.Timestamp) (protobuf *proto.ApbCommitResp) {
	protobuf = &proto.ApbCommitResp{
		Success:    shared.TRUE_POINTER,
		CommitTime: createTxnDescriptorBytes(txnId, ts),
	}
	return
}

func CreateCommitOkRespReuse(txnId TransactionId, ts clocksi.Timestamp, protobuf *proto.ApbCommitResp) *proto.ApbCommitResp {
	*protobuf.Success = true
	createTxnDescriptorBytesReuse(txnId, ts, protobuf.CommitTime)
	return protobuf
}

func CreateCommitFailedResp(errorCode uint32) (protobuf *proto.ApbCommitResp) {
	protobuf = &proto.ApbCommitResp{
		Success:   shared.FALSE_POINTER,
		Errorcode: pb.Uint32(errorCode),
	}
	return
}

// : Check if these replies are being given just like in antidote (i.e., same arguments in case of success/failure, etc.)
// func CreateStaticReadResp(readReplies []*proto.ApbReadObjectResp, ts clocksi.Timestamp) (protobuf *proto.ApbStaticReadObjectsResp) {
func CreateStaticReadResp(objectStates []crdt.State, txnId TransactionId, ts clocksi.Timestamp, buf *crdt.BufsToReturnToPool) (protobuf *proto.ApbStaticReadObjectsResp) {
	protobuf = &proto.ApbStaticReadObjectsResp{
		Objects:    CreateReadObjectsResp(objectStates, buf),
		Committime: CreateCommitOkResp(txnId, ts),
	}
	return
}

func CreateStaticReadRespReuse(objectStates []crdt.State, txnId TransactionId, ts clocksi.Timestamp, buf *crdt.BufsToReturnToPool, protobuf *proto.ApbStaticReadObjectsResp) *proto.ApbStaticReadObjectsResp {
	protobuf.Objects = CreateReadObjectsRespReuse(objectStates, buf, protobuf.Objects)
	protobuf.Committime = CreateCommitOkRespReuse(txnId, ts, protobuf.Committime)
	return protobuf
}

func CreateReadObjectsResp(objectStates []crdt.State, buf *crdt.BufsToReturnToPool) (protobuf *proto.ApbReadObjectsResp) {
	readReplies := convertAntidoteStatesToProto(objectStates, buf)
	protobuf = &proto.ApbReadObjectsResp{
		Success: shared.TRUE_POINTER,
		Objects: readReplies,
	}
	return
}

func CreateReadObjectsRespReuse(objectStates []crdt.State, buf *crdt.BufsToReturnToPool, protobuf *proto.ApbReadObjectsResp) *proto.ApbReadObjectsResp {
	*protobuf.Success = true
	protobuf.Objects = convertAntidoteStatesToProtoReuse(objectStates, buf, protobuf.Objects)
	return protobuf
}

func CreateOperationResp() (protobuf *proto.ApbOperationResp) {
	protobuf = &proto.ApbOperationResp{
		Success: shared.TRUE_POINTER,
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
		txnId, ts = /*TransactionId(rand.Uint64())*/ TransactionId(0), clocksi.SliceTimestamp{}.FromBytes(bytes)
	} else {
		txnId, ts = TransactionId(binary.BigEndian.Uint64(bytes[0:8])), clocksi.SliceTimestamp{}.FromBytes(bytes[8:])
	}
	return
}

func DecodeTxnDescriptorReuse(bytes []byte, reuseTs clocksi.Timestamp) (txnId TransactionId, ts clocksi.Timestamp) {
	if len(bytes) == 0 {
		//FromBytes of clocksi can handle nil arrays
		return TransactionId(0), reuseTs.FromBytesInto(bytes)
	}
	return TransactionId(binary.BigEndian.Uint64(bytes[0:8])), reuseTs.FromBytesInto(bytes[8:])
}

func ProtoObjectsToAntidoteObjectsReuse(protoObjs []*proto.ApbBoundObject, reuseP []crdt.ReadObjectParams) (objs []crdt.ReadObjectParams) {
	if len(protoObjs) > cap(reuseP) {
		reuseP = make([]crdt.ReadObjectParams, len(protoObjs))
	}
	objs = reuseP[:len(protoObjs)]
	for i, currObj := range protoObjs {
		objs[i] = crdt.ReadObjectParams{
			KeyParams: crdt.MakeKeyParams(tools.UnsafeBytesToString(currObj.GetKey()), currObj.GetType(), tools.UnsafeBytesToString(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{},
		}
	}
	return
}

func ProtoObjectsToAntidoteObjects(protoObjs []*proto.ApbBoundObject) (objs []crdt.ReadObjectParams) {
	objs = make([]crdt.ReadObjectParams, len(protoObjs))

	for i, currObj := range protoObjs {
		objs[i] = crdt.ReadObjectParams{
			//KeyParams: crdt.MakeKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			KeyParams: crdt.MakeKeyParams(tools.UnsafeBytesToString(currObj.GetKey()), currObj.GetType(), tools.UnsafeBytesToString(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{},
		}
	}
	return
}

func ProtoReadToAntidoteObjectsReuse(fullReads []*proto.ApbBoundObject, partialReads []*proto.ApbPartialRead, reuseP []crdt.ReadObjectParams) (objs []crdt.ReadObjectParams) {
	totalLen := len(fullReads) + len(partialReads)
	if totalLen > cap(reuseP) {
		reuseP = make([]crdt.ReadObjectParams, totalLen)
	}
	objs = reuseP[:totalLen]

	for i, currObj := range fullReads {
		objs[i] = crdt.ReadObjectParams{
			KeyParams: crdt.MakeKeyParams(tools.UnsafeBytesToString(currObj.GetKey()), currObj.GetType(), tools.UnsafeBytesToString(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{},
		}
	}

	offset := len(fullReads)
	var boundObj *proto.ApbBoundObject
	for i, currObj := range partialReads {
		boundObj = currObj.GetObject()
		objs[i+offset] = crdt.ReadObjectParams{
			KeyParams: crdt.MakeKeyParams(tools.UnsafeBytesToString(boundObj.GetKey()), boundObj.GetType(), tools.UnsafeBytesToString(boundObj.GetBucket())),
			ReadArgs:  crdt.PartialReadOpToAntidoteRead(currObj.GetArgs(), boundObj.GetType(), currObj.GetReadtype()),
		}
	}
	return
}

func ProtoReadToAntidoteObjects(fullReads []*proto.ApbBoundObject, partialReads []*proto.ApbPartialRead) (objs []crdt.ReadObjectParams) {
	objs = make([]crdt.ReadObjectParams, len(fullReads)+len(partialReads))

	for i, currObj := range fullReads {
		objs[i] = crdt.ReadObjectParams{
			//KeyParams: crdt.MakeKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			KeyParams: crdt.MakeKeyParams(tools.UnsafeBytesToString(currObj.GetKey()), currObj.GetType(), tools.UnsafeBytesToString(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{},
		}
	}

	var boundObj *proto.ApbBoundObject
	for i, currObj := range partialReads {
		boundObj = currObj.GetObject()
		objs[i+len(fullReads)] = crdt.ReadObjectParams{
			//KeyParams: crdt.MakeKeyParams(string(boundObj.GetKey()), boundObj.GetType(), string(boundObj.GetBucket())),
			KeyParams: crdt.MakeKeyParams(tools.UnsafeBytesToString(boundObj.GetKey()), boundObj.GetType(), tools.UnsafeBytesToString(boundObj.GetBucket())),
			ReadArgs:  crdt.PartialReadOpToAntidoteRead(currObj.GetArgs(), boundObj.GetType(), currObj.GetReadtype()),
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
			//KeyParams:  crdt.MakeKeyParams(tools.UnsafeBytesToString(currObj.GetKey()), currObj.GetType(), tools.UnsafeBytesToString(currObj.GetBucket())),
			KeyParams:  crdt.MakeKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			UpdateArgs: crdt.UpdateProtoToAntidoteUpdate(currUpOp, currObj.GetType()),
		}
	}

	return
}

func ProtoUpdateOpToAndidoteUpdateReuse(protoUp []*proto.ApbUpdateOp, reuseParams []crdt.UpdateObjectParams) (updParams []crdt.UpdateObjectParams) {
	if len(protoUp) <= cap(reuseParams) { //Fastest path, can fully re-use
		var currObj *proto.ApbBoundObject = nil
		var currUpOp *proto.ApbUpdateOperation = nil
		updParams = reuseParams[:len(protoUp)]
		for i, update := range protoUp {
			currObj, currUpOp = update.GetBoundobject(), update.GetOperation()
			updParams[i] = crdt.UpdateObjectParams{
				KeyParams:  crdt.MakeKeyParams(tools.UnsafeBytesToString(currObj.GetKey()), currObj.GetType(), tools.UnsafeBytesToString(currObj.GetBucket())),
				UpdateArgs: crdt.UpdateProtoToAntidoteUpdate(currUpOp, currObj.GetType()),
			}
		}
		return
	} else { //Will need to alloc new sadly. No point re-using as assigning new crdt.UpdateObjectParams is lightweight.
		return ProtoUpdateOpToAntidoteUpdate(protoUp)
	}
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
	utilities.CheckErr("Error unmarshaling received protobuf", err)
	return
}

func unmarshallProtoVTClientBuf(code byte, msgBuf []byte, clientBuf *proto.PbBuffers) (normalPb pb.Message) {
	var protobuf proto.VTMessage
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
		//protobuf = &proto.ApbStaticUpdateObjects{}
		if clientBuf.StaticUpd == nil {
			clientBuf.UpdateInit()
		}
		err := clientBuf.StaticUpd.UnmarshalVTUnsafeReuse(msgBuf, clientBuf)
		utilities.CheckErr("Error unmarshalling received protobuf (reuse)", err)
		return clientBuf.StaticUpd
		/*protobuf = &proto.ApbStaticUpdateObjects{}
		err := protobuf.UnmarshalVT(msgBuf)
		utilities.CheckErr("Error unmarshalling received protobuf", err)
		return protobuf.(pb.Message)*/
	case StaticReadObjs:
		err := clientBuf.StaticReadObjects.UnmarshalVTUnsafeReuse(msgBuf, clientBuf)
		utilities.CheckErr("Error unmarshalling received protobuf (reuse)", err)
		return clientBuf.StaticReadObjects
	case StaticRead:
		err := clientBuf.StaticRead.UnmarshalVTUnsafeReuse(msgBuf, clientBuf)
		utilities.CheckErr("Error unmarshalling received protobuf (reuse)", err)
		return clientBuf.StaticRead
	case ResetServer:
		protobuf = &proto.ApbResetServer{}
	case NewTrigger:
		protobuf = &proto.ApbNewTrigger{}
	case GetTriggers:
		protobuf = &proto.ApbGetTriggers{}
	case ServerConn:
		protobuf = &proto.ApbServerConn{}
	case S2S:
		err := clientBuf.S2SReq.UnmarshalVTOptSafeReuse(msgBuf)
		utilities.CheckErr("Error unmarshalling received protobuf", err)
		return clientBuf.S2SReq
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
		err := clientBuf.S2SReply.UnmarshalVTOptSafeReuse(msgBuf)
		utilities.CheckErr("Error unmarshalling received protobuf", err)
		return clientBuf.S2SReply
	case MultiConnectReply:
		protobuf = &proto.ApbMultiClientConnectResp{}
	case ErrorReply:
		protobuf = &proto.ApbErrorResp{}
	}
	/*if code != StaticUpdateObjs {
		fmt.Printf("[APL]Unmarshalling code %d @unmarshallProtoVTClientBuf.\n", code)
	}*/
	var err error
	if code == UpdateObjs {
		//Can't use unsafe, as we'll store data when downstreaming.
		err = protobuf.UnmarshalVT(msgBuf)
	} else {
		err = protobuf.UnmarshalVTUnsafe(msgBuf)
		//err = protobuf.UnmarshalVT(msgBuf)
	}
	//fmt.Println(code)
	//err := protobuf.UnmarshalVTUnsafe(msgBuf)
	//fmt.Println(protobuf)
	utilities.CheckErr("Error unmarshaling received protobuf", err)
	normalPb = protobuf.(pb.Message)
	return
}

func unmarshallProtoVT(code byte, msgBuf []byte) (normalPb pb.Message) {
	var protobuf proto.VTMessage
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
		/*s2sP := &proto.S2SWrapper{}
		//err := s2sP.UnmarshalVTSafe(msgBuf) //S2S can only be unmarshalled with unsafe if it is not an update.
		err := s2sP.UnmarshalVT(msgBuf)
		utilities.CheckErr("Error unmarshalling received protobuf", err)
		return s2sP*/
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
		/*s2sP := &proto.S2SWrapperReply{}
		/*err := s2sP.UnmarshalVT(msgBuf)
		utilities.CheckErr("Error unmarshalling received protobuf", err)
		return s2sP*/
	case MultiConnectReply:
		protobuf = &proto.ApbMultiClientConnectResp{}
	case ErrorReply:
		protobuf = &proto.ApbErrorResp{}
	}
	//fmt.Println(code)
	var err error
	if code == StaticUpdateObjs || code == UpdateObjs || code == S2S || code == S2SReply {
		//Can't use unsafe, as we'll store data when downstreaming.
		//S2S is also not safe as input buffers get re-used before they are processed (multiple clients share the same connection)
		err = protobuf.UnmarshalVT(msgBuf)
	} else {
		//err = protobuf.UnmarshalVT(msgBuf)
		err = protobuf.UnmarshalVTUnsafe(msgBuf)
	}
	//copyBuf := make([]byte, len(msgBuf))
	//copy(copyBuf, msgBuf)
	//err := protobuf.UnmarshalVTUnsafe(copyBuf)
	//err := protobuf.UnmarshalVT(msgBuf)
	//fmt.Println(protobuf)
	utilities.CheckErr("Error unmarshaling received protobuf", err)
	normalPb = protobuf.(pb.Message)
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
	return &proto.ApbBoundObject{Key: tools.UnsafeStringToBytes(key), Type: crdtType.GetSharedPointer(), Bucket: tools.UnsafeStringToBytes(bucket)}
	//return &proto.ApbBoundObject{Key: []byte(key), Type: crdtType.Enum(), Bucket: []byte(bucket)}
}

func createBoundObjectReuse(key string, crdtType proto.CRDTType, bucket string, reuseProto *proto.ApbBoundObject) {
	reuseProto.Key, reuseProto.Type, reuseProto.Bucket = tools.UnsafeStringToBytes(key), crdtType.GetSharedPointer(), tools.UnsafeStringToBytes(bucket)
}

func createTxnDescriptorBytes(txnId TransactionId, ts clocksi.Timestamp) (bytes []byte) {
	/*tsBytes := ts.ToBytes()
	bytes = make([]byte, len(tsBytes)+8)
	binary.BigEndian.PutUint64(bytes[0:8], uint64(txnId))
	copy(bytes[8:], tsBytes)
	return*/
	bytes = make([]byte, ts.GetBytesSize()+8)        //+8: txnID
	binary.BigEndian.PutUint64(bytes, uint64(txnId)) //Writes txnID in the first 8 bytes
	ts.ToBytesBuf(bytes[8:])                         //Writes timestamp in the rest of the array
	return
}

func createTxnDescriptorBytesReuse(txnId TransactionId, ts clocksi.Timestamp, reuseBytes []byte) {
	binary.BigEndian.PutUint64(reuseBytes, uint64(txnId))
	ts.ToBytesBuf(reuseBytes[8:])
}

func convertAntidoteStatesToProtoReuse(objectStates []crdt.State, buf *crdt.BufsToReturnToPool, reuseProtos []*proto.ApbReadObjectResp) (protobufs []*proto.ApbReadObjectResp) {
	if cap(reuseProtos) < len(objectStates) {
		reuseProtos = make([]*proto.ApbReadObjectResp, len(objectStates)+len(objectStates)/10) //Slight more space to avoid further resizes.
	}
	protobufs = reuseProtos[:len(objectStates)]
	for i, state := range objectStates {
		protobufs[i] = state.(crdt.ProtoState).ToReadResp(buf)
	}
	return
}

func convertAntidoteStatesToProto(objectStates []crdt.State, buf *crdt.BufsToReturnToPool) (protobufs []*proto.ApbReadObjectResp) {
	protobufs = make([]*proto.ApbReadObjectResp, len(objectStates))
	for i, state := range objectStates {
		//fmt.Printf("State to add to reply: %T %v\n", state, state)
		protobufs[i] = state.(crdt.ProtoState).ToReadResp(buf)
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
