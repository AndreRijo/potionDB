package main

//https://opensource.com/article/18/5/building-concurrent-tcp-server-go
//https://golang.org/pkg/net/
//To run in vscode: ctrl+option+N. This *should* work when it actually updates $GOPATH
//From terminal: go to main folder and then: go run protoServer.go simpleClient.go

//TODO: Reuse of canals? Creating a new canal for each read/write seems like a waste... Should I ask the teachers?

import (
	"antidote"
	"bufio"
	"clocksi"
	"crdt"
	"fmt"
	rand "math/rand"
	"net"
	"os"
	"strings"
	"time"
	"tools"

	proto "github.com/golang/protobuf/proto"
)

var (
	in = bufio.NewReader(os.Stdin)
)

func main() {

	fmt.Println("Port?")
	portString, _ := in.ReadString('\n')
	rand.Seed(time.Now().UTC().UnixNano())
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
	tmChan := antidote.CreateClientHandler()
	//TODO: Change this to a random ID generated inside the transaction. This ID should be different from transaction to transaction
	//The current solution can give problems in the Materializer when a commited transaction is put on hold and another transaction from the same client arrives
	var clientId antidote.ClientId = antidote.ClientId(rand.Uint64())

	var replyType byte = 0
	var reply proto.Message = nil
	for {
		//TODO: Handle when client breaks connection or sends invalid data
		//Possible invalid data case (e.g.): sends code for "StaticUpdateObjs" but instead sends a protobuf of another type (e.g: StartTrans)
		//Read protobuf
		fmt.Println("Waiting for client's request...")
		protoType, protobuf, err := antidote.ReceiveProto(conn)
		//This works in MacOS, but not on windows. For now we'll add any error here
		//if err == io.EOF
		if err != nil {
			fmt.Println("Connection closed by client.")
			tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
			return
		}
		tools.CheckErr(tools.NETWORK_READ_ERROR, err)
		switch protoType {
		case antidote.ReadObjs:
			fmt.Println("Received proto of type ApbReadObjects")
			replyType = antidote.ReadObjsReply
			reply = handleReadObjects(protobuf.(*antidote.ApbReadObjects), tmChan, clientId)
		case antidote.UpdateObjs:
			fmt.Println("Received proto of type ApbUpdateObjects")
			//TODO: Check what antidote replies on this case
			replyType = antidote.ErrorReply
			reply = handleUpdateObjects(protobuf.(*antidote.ApbUpdateObjects), tmChan, clientId)
		case antidote.StartTrans:
			fmt.Println("Received proto of type ApbStartTransaction")
			replyType = antidote.StartTransReply
			reply = handleStartTxn(protobuf.(*antidote.ApbStartTransaction), tmChan, clientId)
		case antidote.AbortTrans:
			fmt.Println("Received proto of type ApbAbortTransaction")
			//TODO: Check what antidote replies on this case
			replyType = antidote.CommitTransReply
			reply = handleAbortTxn(protobuf.(*antidote.ApbAbortTransaction), tmChan, clientId)
		case antidote.CommitTrans:
			fmt.Println("Received proto of type ApbCommitTransaction")
			replyType = antidote.CommitTransReply
			reply = handleCommitTxn(protobuf.(*antidote.ApbCommitTransaction), tmChan, clientId)
		case antidote.StaticUpdateObjs:
			fmt.Println("Received proto of type ApbStaticUpdateObjects")
			replyType = antidote.CommitTransReply
			reply = handleStaticUpdateObjects(protobuf.(*antidote.ApbStaticUpdateObjects), tmChan, clientId)
		case antidote.StaticReadObjs:
			fmt.Println("Received proto of type ApbStaticReadObjects")
			replyType = antidote.StaticReadObjsReply
			reply = handleStaticReadObjects(protobuf.(*antidote.ApbStaticReadObjects), tmChan, clientId)
		default:
			fmt.Println("Received unknown proto, ignored... sort of")
		}
		//TODO: Handle errors that may be returned due to incorrect parameters/internal errors
		fmt.Println("Sending reply proto")
		antidote.SendProto(replyType, reply, conn)
	}
}

//TODO: Error cases in which it should return ApbErrorResp
func handleStaticReadObjects(proto *antidote.ApbStaticReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbStaticReadObjectsResp) {

	clientClock := clocksi.ClockSiTimestamp{}.FromBytes(proto.GetTransaction().GetTimestamp())

	objs := protoObjectsToAntidoteObjects(proto.GetObjects())
	replyChan := make(chan antidote.TMReadReply)

	tmChan <- antidote.TransactionManagerRequest{
		Args: antidote.TMReadArgs{
			ObjsParams: objs,
			ReplyChan:  replyChan,
		},
		TransactionId: antidote.TransactionId{
			ClientId:  clientId,
			Timestamp: clientClock,
		},
	}
	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateStaticReadResp(reply.States, reply.Timestamp)
	return
}


func protoObjectsToAntidoteObjects(protoObjs []*antidote.ApbBoundObject) (objs []antidote.KeyParams) {

	objs = make([]antidote.KeyParams, len(protoObjs))

	for i, currObj := range protoObjs {
		objs[i] = antidote.CreateKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket()))
	}
	return
}

//TODO: Error cases in which it should return ApbErrorResp
func handleStaticUpdateObjects(proto *antidote.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbCommitResp) {

	clientClock := clocksi.ClockSiTimestamp{}.FromBytes(proto.GetTransaction().GetTimestamp())

	updates := protoUpdateOpToAntidoteUpdate(proto.GetUpdates())
	replyChan := make(chan antidote.TMUpdateReply)

	tmChan <- antidote.TransactionManagerRequest{
		Args: antidote.TMUpdateArgs{
			UpdateParams: updates,
			ReplyChan:    replyChan,
		},
		TransactionId: antidote.TransactionId{
			ClientId:  clientId,
			Timestamp: clientClock,
		},
	}

	reply := <-replyChan
	//TODO: Actually not ignore error
	ignore(reply.Err)

	respProto = antidote.CreateCommitOkResp(reply.Timestamp)
	return
}

func handleReadObjects(proto *antidote.ApbReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbReadObjectsResp) {
	notSupported(proto)
	return nil
}

func handleUpdateObjects(proto *antidote.ApbUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbErrorResp) {
	notSupported(proto)
	return nil
}

func handleStartTxn(proto *antidote.ApbStartTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbStartTransactionResp) {
	notSupported(proto)
	return nil
}

func handleAbortTxn(proto *antidote.ApbAbortTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbCommitResp) {
	notSupported(proto)
	return nil
}

func handleCommitTxn(proto *antidote.ApbCommitTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbCommitResp) {
	notSupported(proto)
	return nil
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
	case antidote.CRDTType_ORSET:
		setProto := protoOperation.GetSetop()
		if setProto.GetOptype() == antidote.ApbSetUpdate_ADD {
			updateArgs = crdt.AddAll{Elems: crdt.ByteMatrixToElementArray(setProto.GetAdds())}
		} else {
			updateArgs = crdt.RemoveAll{Elems: crdt.ByteMatrixToElementArray(setProto.GetRems())}
		}
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

func notSupported(protobuf proto.Message) {
	fmt.Println("Received proto is recognized but not yet supported")
}

//Temporary method. This is used to avoid compile errors on unused variables
//This unused variables mark stuff that isn't being processed yet.
func ignore(any interface{}) {

}
