package main

//https://opensource.com/article/18/5/building-concurrent-tcp-server-go
//https://golang.org/pkg/net/
//To run in vscode: ctrl+option+N. This *should* work when it actually updates $GOPATH
//From terminal: go to main folder and then: go run protoServer.go simpleClient.go

import (
	"antidote"
	"bufio"
	"clocksi"
	"crdt"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"tools"

	proto "github.com/golang/protobuf/proto"
)

var (
	in = bufio.NewReader(os.Stdin)
)

func main() {

	//TODO: Remove this client parts. It's temporary, just until I decide to separate the client from the server in different projects.
	fmt.Println("What to run? Client or server?")
	toRun, _ := in.ReadString('\n')
	fmt.Println(toRun)
	if strings.ToLower(strings.TrimSpace(toRun)) == "client" {
		fmt.Println("Starting client")
		ClientMain()
		return
	}
	fmt.Println("Port?")
	portString, _ := in.ReadString('\n')
	antidote.Initialize()
	server, err := net.Listen("tcp", "127.0.0.1:"+strings.TrimSpace(portString))
	tools.CheckErr(tools.PORT_ERROR, err)

	//Stop listening to port on shutdown
	defer server.Close()

	for {
		conn, err := server.Accept()
		tools.CheckErr(tools.NEW_CONN_ERROR, err)
		go processConnection(conn)
	}
}

/*
Handles a connection initiated by a new client.
Connection protocol (for both client and server):
msgSize (int32), msgType (1 byte), protobuf
Note that this is the same interaction type as in antidote.

conn - the TCP connection between the client and this server.
*/
func processConnection(conn net.Conn) {
	var replyType byte = 0
	var reply proto.Message = nil
	for {
		//TODO: Handle when client breaks connection or sends invalid data
		//Possible invalid data case (e.g.): sends code for "StaticUpdateObjs" but instead sends a protobuf of another type (e.g: StartTrans)
		//Read protobuf
		fmt.Println("Waiting for client's request...")
		protoType, protobuf, err := antidote.ReceiveProto(conn)
		if err == io.EOF {
			fmt.Println("Connection closed by client.")
			return
		}
		tools.CheckErr(tools.NETWORK_READ_ERROR, err)

		switch protoType {
		case antidote.ReadObjs:
			notSupported(protobuf)
		case antidote.UpdateObjs:
			notSupported(protobuf)
		case antidote.StartTrans:
			notSupported(protobuf)
		case antidote.AbortTrans:
			notSupported(protobuf)
		case antidote.CommitTrans:
			notSupported(protobuf)
		case antidote.StaticUpdateObjs:
			fmt.Println("Received proto of type StaticUpdateObjs")
			replyType = antidote.CommitTransReply
			reply = handleStaticUpdateObjects(protobuf.(*antidote.ApbStaticUpdateObjects))
		case antidote.StaticReadObjs:
			fmt.Println("Received proto of type StaticReadObjs")
			replyType = antidote.StaticReadObjsReply
			reply = handleStaticReadObjects(protobuf.(*antidote.ApbStaticReadObjects))
		default:
			fmt.Println("Received unknown proto, ignored... sort of")
		}
		//TODO: Handle errors that may be returned due to incorrect parameters/internal errors
		fmt.Println("Sending reply proto")
		antidote.SendProto(replyType, reply, conn)
	}
}

func notSupported(protobuf proto.Message) {
	fmt.Println("Received proto is recognized but not yet supported")
}

//TODO: Error cases in which it should return ApbErrorResp
func handleStaticReadObjects(proto *antidote.ApbStaticReadObjects) (respProto *antidote.ApbStaticReadObjectsResp) {
	clientClock := clocksi.FromBytes(proto.GetTransaction().GetTimestamp())

	objs := protoObjectsToAntidoteObjects(proto.GetObjects())
	replies, ts := antidote.HandleStaticReadObjects(objs, clientClock)

	respProto = antidote.CreateStaticReadResp(replies, ts)
	return
}

func protoObjectsToAntidoteObjects(protoObjs []*antidote.ApbBoundObject) (objs []antidote.ReadObjectParams) {
	objs = make([]antidote.ReadObjectParams, len(protoObjs))

	for i, currObj := range protoObjs {
		objs[i] = antidote.ReadObjectParams{
			KeyParams: antidote.CreateKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
		}
	}
	return
}

//TODO: Error cases in which it should return ApbErrorResp
func handleStaticUpdateObjects(proto *antidote.ApbStaticUpdateObjects) (respProto *antidote.ApbCommitResp) {

	clientClock := clocksi.FromBytes(proto.GetTransaction().GetTimestamp())

	updates := protoUpdateOpToAntidoteUpdate(proto.GetUpdates())
	ts, err := antidote.HandleStaticUpdateObjects(updates, clientClock)

	//TODO: Actually not ignore error
	ignore(err)

	respProto = antidote.CreateCommitOkResp(ts)
	return
}

//TODO: Maybe the two methods below should be moved to protoLib?
func protoUpdateOpToAntidoteUpdate(protoUp []*antidote.ApbUpdateOp) (upParams []antidote.UpdateObjectParams) {
	upParams = make([]antidote.UpdateObjectParams, len(protoUp))
	var currObj *antidote.ApbBoundObject = nil
	var currUpOp *antidote.ApbUpdateOperation = nil

	for i, update := range protoUp {
		currObj, currUpOp = update.GetBoundobject(), update.GetOperation()
		upParams[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.CreateKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			UpdateArgs: protoUpdateOperationToAntidoteArguments(currUpOp, currObj.GetType()),
		}
	}

	return
}

//TODO: Should this be moved to the CRDT code? Or left here?
func protoUpdateOperationToAntidoteArguments(protoOperation *antidote.ApbUpdateOperation,
	crdtType antidote.CRDTType) (updateArgs crdt.UpdateArguments) {
	switch crdtType {
	case antidote.CRDTType_COUNTER:
		updateArgs = crdt.Increment{Change: int32(protoOperation.GetCounterop().GetInc())}
	default:
		//TODO: Support other types and error case, and return error to client
		tools.CheckErr("Unsupported data type for update - we should warn the user about this one day.", nil)
	}

	return
}

/*
messageTypeToCode('ApbErrorResp')             -> 0;
messageTypeToCode('ApbRegUpdate')             -> 107;
messageTypeToCode('ApbGetRegResp')            -> 108;
messageTypeToCode('ApbCounterUpdate')         -> 109;
messageTypeToCode('ApbGetCounterResp')        -> 110;
messageTypeToCode('ApbOperationResp')         -> 111;
messageTypeToCode('ApbSetUpdate')             -> 112;
messageTypeToCode('ApbGetSetResp')            -> 113;
messageTypeToCode('ApbTxnProperties')         -> 114;
messageTypeToCode('ApbBoundObject')           -> 115;
messageTypeToCode('ApbReadObjects')           -> 116;
messageTypeToCode('ApbUpdateOp')              -> 117;
messageTypeToCode('ApbUpdateObjects')         -> 118;
messageTypeToCode('ApbStartTransaction')      -> 119;
messageTypeToCode('ApbAbortTransaction')      -> 120;
messageTypeToCode('ApbCommitTransaction')     -> 121;
messageTypeToCode('ApbStaticUpdateObjects')   -> 122;
messageTypeToCode('ApbStaticReadObjects')     -> 123;
messageTypeToCode('ApbStartTransactionResp')  -> 124;
messageTypeToCode('ApbReadObjectResp')        -> 125;
messageTypeToCode('ApbReadObjectsResp')       -> 126;
messageTypeToCode('ApbCommitResp')            -> 127;
messageTypeToCode('ApbStaticReadObjectsResp') -> 128;
messageTypeToCode('ApbCreateDC')                    -> 129;
messageTypeToCode('ApbConnectToDCs')                -> 130;
messageTypeToCode('ApbGetConnectionDescriptor')     -> 131;
messageTypeToCode('ApbGetConnectionDescriptorResp') -> 132.
*/

//Temporary method. This is used to avoid compile errors on unused variables
//This unused variables mark stuff that isn't being processed yet.
func ignore(any interface{}) {

}
