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
	"strconv"
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
	portString = strings.Trim(portString, "\n")
	rand.Seed(time.Now().UTC().UnixNano())
	id, _ := strconv.ParseInt(portString, 0, 64)

	/*
		fmt.Println("Other servers? Type their IDs, all in the same line separated by a space.")
		otherIDsString, _ := in.ReadString('\n')
		otherIDsString = strings.Trim(otherIDsString, "\n")
		otherReplicasIDs := strings.Split(otherIDsString, " ")
	*/

	tm := antidote.Initialize(id)
	//tm := antidote.Initialize(0)
	/*
		for _, idStr := range otherReplicasIDs {
			remoteID, _ := strconv.ParseInt(idStr, 0, 64)
			tm.AddRemoteID(remoteID)
		}
	*/

	server, err := net.Listen("tcp", "127.0.0.1:"+strings.TrimSpace(portString))

	//server, err := net.Listen("tcp", "127.0.0.1:8087")
	tools.CheckErr(tools.PORT_ERROR, err)
	fmt.Println("PotionDB started.")

	//Stop listening to port on shutdown
	defer server.Close()

	for {
		conn, err := server.Accept()
		tools.CheckErr(tools.NEW_CONN_ERROR, err)
		go processConnection(conn, tm, id)
	}
}

/*
Handles a connection initiated by a new client.
Connection protocol (for both client and server):
msgSize (int32), msgType (1 byte), protobuf
Note that this is the same interaction type as in antidote.

conn - the TCP connection between the client and this server.
*/
func processConnection(conn net.Conn, tm *antidote.TransactionManager, replicaID int64) {
	tmChan := tm.CreateClientHandler()
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
		case antidote.ConnectReplica:
			tools.FancyInfoPrint(tools.INFO, replicaID, "Received proto of type ApbConnectReplica")
			replyType = antidote.ConnectReplicaReply
			reply = handleReplicaConn(protobuf.(*antidote.ApbConnectReplica), tmChan)
		case antidote.ConnectReplicaReply:
			tools.FancyInfoPrint(tools.INFO, replicaID, "Received proto of type ApbConnectReplicaReply")
			handleReplicaConnReply(protobuf.(*antidote.ApbConnectReplicaResp), tmChan)
			//Don't need to send any reply
			continue
		case antidote.ReadObjs:
			fmt.Println("Received proto of type ApbReadObjects")
			replyType = antidote.ReadObjsReply
			reply = handleReadObjects(protobuf.(*antidote.ApbReadObjects), tmChan, clientId)
		case antidote.UpdateObjs:
			fmt.Println("Received proto of type ApbUpdateObjects")
			//TODO: Check what antidote replies on this case
			replyType = antidote.OpReply
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

func handleReplicaConn(proto *antidote.ApbConnectReplica, tmChan chan antidote.TransactionManagerRequest) (respProto *antidote.ApbConnectReplicaResp) {
	/*
		tmChan <- antidote.TransactionManagerRequest{
			Args:
		}
	*/
	return nil
}

func handleReplicaConnReply(proto *antidote.ApbConnectReplicaResp, tmChan chan antidote.TransactionManagerRequest) {

}

//TODO: Error cases in which it should return ApbErrorResp
func handleStaticReadObjects(proto *antidote.ApbStaticReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbStaticReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())

	objs := protoObjectsToAntidoteObjects(proto.GetObjects())
	replyChan := make(chan antidote.TMStaticReadReply)

	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp)
	return
}

//TODO: Error cases in which it should return ApbErrorResp
func handleStaticUpdateObjects(proto *antidote.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbCommitResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())

	updates := protoUpdateOpToAntidoteUpdate(proto.GetUpdates())
	replyChan := make(chan antidote.TMStaticUpdateReply)

	tmChan <- createTMRequest(antidote.TMStaticUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)
	//TODO: Actually not ignore error
	ignore(reply.Err)

	respProto = antidote.CreateCommitOkResp(reply.TransactionId, reply.Timestamp)
	return
}

func handleReadObjects(proto *antidote.ApbReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbReadObjectsResp) {

	//didn't test, but this one should definitelly return ApbReadObjectsResp. Success should also be always true unless there is a type error?

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	objs := protoObjectsToAntidoteObjects(proto.GetBoundobjects())
	replyChan := make(chan []crdt.State)

	tmChan <- createTMRequest(antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateReadObjectsResp(reply)
	return
}

func handleUpdateObjects(proto *antidote.ApbUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbOperationResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	updates := protoUpdateOpToAntidoteUpdate(proto.GetUpdates())
	replyChan := make(chan antidote.TMUpdateReply)

	tmChan <- createTMRequest(antidote.TMUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)
	//TODO: Actually not ignore error
	ignore(reply.Err)

	respProto = antidote.CreateOperationResp()
	return
	//return type 111, success: true. I guess this always returns success unless there is a type error.
}

func handleStartTxn(proto *antidote.ApbStartTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbStartTransactionResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTimestamp())
	replyChan := make(chan antidote.TMStartTxnReply)

	tmChan <- createTMRequest(antidote.TMStartTxnArgs{ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	//Examples of txn descriptors in antidote:
	//{tx_id,1550320956784892,<0.4144.0>}.
	//{tx_id,1550321073482453,<0.4143.0>}. (obtained on the op after the previous timestamp)
	//{tx_id,1550321245370469,<0.4146.0>}. (obtained after deleting the logs)
	//It's basically a timestamp plus some kind of counter?

	//TODO: Consider properties?
	respProto = antidote.CreateStartTransactionResp(reply.TransactionId, reply.Timestamp)
	return
}

func handleAbortTxn(proto *antidote.ApbAbortTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbCommitResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	tmChan <- createTMRequest(antidote.TMAbortArgs{}, txnId, clientClock)

	//TODO: Errors such as transaction does not exist, txn already commited or aborted, etc?
	//TODO: Should I wait for a msg from TM?

	respProto = antidote.CreateCommitOkResp(txnId, clientClock)
	//Returns a clock and success set as true. I assume the clock is the same as the one returned in startTxn?
	return
}

func handleCommitTxn(proto *antidote.ApbCommitTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *antidote.ApbCommitResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())
	replyChan := make(chan antidote.TMCommitReply)

	tmChan <- createTMRequest(antidote.TMCommitArgs{ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan

	//TODO: Errors?
	respProto = antidote.CreateCommitOkResp(txnId, reply.Timestamp)
	return
}

func protoObjectsToAntidoteObjects(protoObjs []*antidote.ApbBoundObject) (objs []antidote.ReadObjectParams) {

	objs = make([]antidote.ReadObjectParams, len(protoObjs))

	for i, currObj := range protoObjs {
		objs[i] = antidote.ReadObjectParams{
			KeyParams: antidote.CreateKeyParams(string(currObj.GetKey()), currObj.GetType(), string(currObj.GetBucket())),
			ReadArgs:  crdt.StateReadArguments{}, //TODO: Implement partial read in proto side
		}
	}
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

func createTMRequest(args antidote.TMRequestArgs, txnId antidote.TransactionId,
	clientClock clocksi.Timestamp) (request antidote.TransactionManagerRequest) {
	return antidote.TransactionManagerRequest{
		Args:          args,
		TransactionId: txnId,
		Timestamp:     clientClock,
	}
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
