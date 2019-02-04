package old

import (
	"fmt"
	"antidote"
	proto "github.com/golang/protobuf/proto"
	"os"
	"net"
	"io"
	"encoding/binary"
)

var (
	i, j int = 0, 10
)

func notmain() {
	lol := "lol"
	for a := 0; a < j; a++ {
		i += a 	
	}
	fmt.Println("derp", "derp", lol, i)
	
	rw := uint32(0)
	rb := uint32(0)
	transProps := &antidote.ApbTxnProperties{
		//ReadWrite: proto.Uint32(0),
		//RedBlue: proto.Uint32(0),
		ReadWrite: &rw,
		RedBlue: &rb,
	}
	startMsg := &antidote.ApbStartTransaction{
		//Timestamp: []byte("1"),
		Properties: transProps,
	}
	
	fmt.Println("timestamp: ", string(startMsg.Timestamp[:]), 
		*transProps.ReadWrite, *transProps.RedBlue)
	
	connection, err := net.Dial("tcp", "127.0.0.1:8087")
	checkErr("Network connection establishment err", err)
	sendStartTransaction(startMsg, connection)
	
	fmt.Println("Waiting for reply...")
	
	_, msgData := receiveProtobuf(connection)
	receivedProto := unmarshallTransResp(msgData)
	fmt.Println("Protobuf succesfully received! Transaction success:", 
		*receivedProto.Success)
	/*
	toReceive := make([]byte, 1024)
	nReceived, err := connection.Read(toReceive)
	checkErr("Error receiving antidote's server reply", err)
	receivedProto := new(antidote.ApbStartTransactionResp)
	err = proto.Unmarshal(toReceive[0:nReceived], receivedProto)
	checkErr("Error unmarshaling received protobuf", err)
	fmt.Println("Protobuf succesfully received! Transaction success:", 
		receivedProto.Success)
	*/
	/*
	test := &example.Test{
			Label: proto.String("hello"),
			Type:  proto.Int32(17),
			Reps:  []int64{1, 2, 3},
		}
		data, err := proto.Marshal(test)
		if err != nil {
			log.Fatal("marshaling err: ", err)
		}
		newTest := &example.Test{}
		err = proto.Unmarshal(data, newTest)
		if err != nil {
			log.Fatal("unmarshaling err: ", err)
		}
		// Now test and newTest contain the same data.
		if test.GetLabel() != newTest.GetLabel() {
			log.Fatalf("data mismatch %q != %q", test.GetLabel(), newTest.GetLabel())
		}
		// etc.
	*/
}

//Every msg sent to antidote has a 5 byte uint header.
//First 4 bytes: msgSize (uint32), 5th: msg type (byte)
func sendStartTransaction(startMsg *antidote.ApbStartTransaction, writer io.Writer) {
	toSend, err := proto.Marshal(startMsg)
	checkErr("Marshal err", err)
	protoSize := len(toSend)
	buffer := make([]byte, protoSize + 5)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(protoSize+1))
	buffer[4] = 119
	_, err = writer.Write(buffer)
	checkErr("Sending protobuf err: %s\n", err)
	fmt.Println("Protobuf sent succesfully!\n")
}

func checkErr(msg string, err error) {
	if (err != nil) {
		fmt.Fprintln(os.Stderr, msg, err)
		os.Exit(1)
	}
}

func receiveProtobuf(in io.Reader) (msgType byte, msgData []byte) {
	sizeBuf := make([]byte, 4)
	n, err := 0, fmt.Errorf("")
	fmt.Println("Starting to read msg size")
	for nRead := 0; nRead < 4; {
		n, err = in.Read(sizeBuf[nRead:])
		checkErr("Error reading antidote's reply header", err)
		nRead += n
		fmt.Println("Read", nRead, "bytes from msg size")
	}
	
	msgSize := (int) (binary.BigEndian.Uint32(sizeBuf))
	msgBuf := make([]byte, msgSize)
	fmt.Println("MsgSize:", msgSize)
	fmt.Println("Starting to read msg contents...")
	for nRead := 0; nRead < msgSize; {
		n, err := in.Read(msgBuf[nRead:])
		checkErr("Error reading antidote's reply", err)
		nRead += n
		fmt.Println("Read", nRead, "bytes from msg content")
	}
	
	msgType = msgBuf[0]
	msgData = msgBuf[1:]
	return
}

func unmarshallTransResp(msgBuf []byte) (protobuf *antidote.ApbStartTransactionResp) {
	protobuf = &antidote.ApbStartTransactionResp{}
	err := proto.Unmarshal(msgBuf[:], protobuf)
	checkErr("Error unmarshaling received protobuf", err)
	return
}

//<>
//TODO: method which returns a generic protobuf msg. 
//This can be used as generic: proto.Message