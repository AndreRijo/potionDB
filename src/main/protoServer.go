package main

//https://opensource.com/article/18/5/building-concurrent-tcp-server-go
//https://golang.org/pkg/net/
//To run in vscode: ctrl+option+N. This *should* work when it actually updates $GOPATH
//From terminal: go to main folder and then: go run protoServer.go simpleClient.go

//profiling: https://github.com/google/pprof/blob/master/doc/README.md
//go tool pprof -http=localhost:36234 ../../profiles/8087/mem.prof

//TODO: Reuse of canals? Creating a new canal for each read/write seems like a waste... Should I ask the advisors?

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	rand "math/rand"
	"net"
	"os"
	"os/signal"
	"potionDB/src/antidote"
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"potionDB/src/shared"
	"potionDB/src/tools"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "github.com/golang/protobuf/proto"
)

var (
	in         = bufio.NewReader(os.Stdin)
	profileCPU bool
	profileMem bool
)

const (
	//Keys for configs
	PORT_KEY         = "protoPort"
	MEM_DEBUG        = "memDebug"
	DO_JOIN          = "doJoin"
	DO_TPCH_DATALOAD = "doDataload"
	CPU_PROFILE_KEY  = "withCPUProfile"
	MEM_PROFILE_KEY  = "withMemProfile"
	CPU_FILE_KEY     = "cpuProfileFile"
	MEM_FILE_KEY     = "memProfileFile"
)

var trash []byte

func main() {
	//debug.SetGCPercent(-1)

	rand.Seed(time.Now().UTC().UnixNano())
	configs := loadConfigs()
	trash = make([]byte, configs.GetIntConfig("initialMem", 0))
	floatSize := float64(len(trash))
	fmt.Printf("Setting an initial empty array of size %.4f GB\n", floatSize/1000000000)
	startProfiling(configs)

	portString := configs.GetOrDefault(PORT_KEY, "8887")
	ports := strings.Split(portString, " ")
	//tmpId, _ := strconv.ParseInt(portString, 0, 64)
	//tmpId2, _ := strconv.ParseInt(configs.GetConfig("potionDBID"), 10, 64)
	//id := int16((tmpId + tmpId2) % math.MaxInt16)
	tmpId, _ := strconv.ParseInt(configs.GetConfig("potionDBID"), 10, 64)
	id := int16(tmpId % (math.MaxInt16 * 2))
	shared.ReplicaID = id

	antidote.SetVMToUse()
	tm := antidote.Initialize(id)
	handleTC(configs)

	doDataload := configs.GetBoolConfig(DO_TPCH_DATALOAD, false)
	fmt.Println(configs.GetConfig(DO_TPCH_DATALOAD))
	dp := antidote.DataloadParameters{}
	if doDataload {
		fmt.Println("[PS]DoDataload")
		sf, dataLoc, region := configs.GetFloatConfig("scale", 1.0), configs.GetConfig("dataLoc"), int8(configs.GetIntConfig("region", -1))
		dp.Region, dp.Sf, dp.DataLoc, dp.Tm, dp.IsTMReady = region, sf, dataLoc, tm, make(chan bool, 1)
		go antidote.LoadData(dp)
	} else {
		fmt.Println("[PS]Not doing dataload")
	}

	//TODO: Remove
	//prepareTopKProtobuf()

	fmt.Println("ReplicaID:", id)
	//Wait for joining mechanism, if it's enabled
	if configs.GetBoolConfig(DO_JOIN, true) {
		fmt.Println("Joining existing servers, please stand by...")
		tm.WaitUntilReady()
		fmt.Println("Join complete, starting PotionDB.")
	} else {
		fmt.Println("Waiting for replicaIDs of existing replicas...")
		tm.WaitUntilReady()
		fmt.Println("All replicaIDs are now known, starting PotionDB.")
	}

	if doDataload {
		dp.IsTMReady <- true
	}

	if len(ports) > 1 {
		for _, port := range ports[1:] {
			go startListener(port, id, tm)
		}
	}
	go debugMemory(configs)
	stopProfiling(configs)

	startListener(ports[0], id, tm)
}

func startListener(port string, id int16, tm *antidote.TransactionManager) {
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(port))

	tools.CheckErr(tools.PORT_ERROR, err)
	fmt.Println("PotionDB started at port", port, "with ReplicaID", id)

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
func processConnection(conn net.Conn, tm *antidote.TransactionManager, replicaID int16) {
	tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Accepted connection.")
	tmChan := tm.CreateClientHandler()
	//TODO: Change this to a random ID generated inside the transaction. This ID should be different from transaction to transaction
	//The current solution can give problems in the Materializer when a commited transaction is put on hold and another transaction from the same client arrives
	var clientId antidote.ClientId = antidote.ClientId(rand.Uint64())
	clientCI := antidote.CodingInfo{}.Initialize()

	var replyType byte = 0
	var reply pb.Message = nil
	var s2sChan chan antidote.TMS2SReply = nil //Used if this is a server to server communication
	for {
		//TODO: Handle when client breaks connection or sends invalid data
		//Possible invalid data case (e.g.): sends code for "StaticUpdateObjs" but instead sends a protobuf of another type (e.g: StartTrans)
		//Read protobuf
		tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Waiting for client's request...")
		protoType, protobuf, err := antidote.ReceiveProto(conn)
		//This works in MacOS, but not on windows. For now we'll add any error here
		//if err == io.EOF
		if err != nil {
			if err == io.EOF {
				/*
					date := time.Now().String()
					tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Connection closed by client.")
					fmt.Println("[ProtoServer]Connection closed by client, shutting down handler. Leaving connection open however at timestamp" + date)
				*/
				tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
				conn.Close()
			} else {
				date := time.Now().String()
				fmt.Printf("[ProtoServer]Error on reading proto from client, putting connection to sleep. Type: %v, proto: %v, error: %s, time: %s\n", protoType, protobuf, err, date)
				time.Sleep(3 * time.Second)
				fmt.Println("[ProtoServer]Closing connection due to error at time", time.Now().String(), ".But for now, actually leaving connection open.")
				tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
				conn.Close()
			}
			return
		}
		tools.CheckErr(tools.NETWORK_READ_ERROR, err)

		switch protoType {
		case antidote.ReadObjs:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbReadObjects")
			replyType = antidote.ReadObjsReply
			reply = handleReadObjects(protobuf.(*proto.ApbReadObjects), tmChan, clientId)
		case antidote.Read:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbRead")
			replyType = antidote.ReadObjsReply
			reply = handleRead(protobuf.(*proto.ApbRead), tmChan, clientId)
		case antidote.UpdateObjs:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbUpdateObjects")
			replyType = antidote.OpReply
			reply = handleUpdateObjects(protobuf.(*proto.ApbUpdateObjects), tmChan, clientId)
		case antidote.StartTrans:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbStartTransaction")
			replyType = antidote.StartTransReply
			reply = handleStartTxn(protobuf.(*proto.ApbStartTransaction), tmChan, clientId)
		case antidote.AbortTrans:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbAbortTransaction")
			replyType = antidote.CommitTransReply
			reply = handleAbortTxn(protobuf.(*proto.ApbAbortTransaction), tmChan, clientId)
		case antidote.CommitTrans:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbCommitTransaction")
			replyType = antidote.CommitTransReply
			reply = handleCommitTxn(protobuf.(*proto.ApbCommitTransaction), tmChan, clientId)
		case antidote.StaticUpdateObjs:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbStaticUpdateObjects")
			replyType = antidote.CommitTransReply
			reply = handleStaticUpdateObjects(protobuf.(*proto.ApbStaticUpdateObjects), tmChan, clientId)
		case antidote.StaticReadObjs:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbStaticReadObjects")
			replyType = antidote.StaticReadObjsReply
			//antidote.SendProtoMarshal(replyType, defaultTopKMarshal, conn)
			//continue
			//reply = defaultTopKProto
			reply = handleStaticReadObjects(protobuf.(*proto.ApbStaticReadObjects), tmChan, clientId)
		case antidote.StaticRead:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbStaticRead")
			replyType = antidote.StaticReadObjsReply
			//antidote.SendProtoMarshal(replyType, defaultTopKMarshal, conn)
			//continue
			//reply = defaultTopKProto
			reply = handleStaticRead(protobuf.(*proto.ApbStaticRead), tmChan, clientId)
		case antidote.NewTrigger:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbNewTrigger")
			replyType = antidote.NewTriggerReply
			reply = handleNewTrigger(protobuf.(*proto.ApbNewTrigger), tmChan, clientId, clientCI)
		case antidote.GetTriggers:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbGetTriggers")
			replyType = antidote.GetTriggersReply
			reply = handleGetTriggers(protobuf.(*proto.ApbGetTriggers), tmChan, clientId, clientCI)
		case antidote.ResetServer:
			fmt.Println("Starting to reset PotionDB")
			replyType = antidote.ResetServerReply
			reply = handleResetServer(tm)
		case antidote.ServerConn:
			s2sChan = handleServerConn(tmChan, conn)
			continue
		case antidote.S2S:
			handleServerToServer(protobuf.(*proto.S2SWrapper), tmChan, s2sChan, conn, tm)
			continue
		default:
			tools.FancyErrPrint(tools.PROTO_PRINT, replicaID, "Received unknown proto, ignored... sort of")
			fmt.Println("I don't know how to handle this proto", protoType)
		}
		tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Sending reply proto")
		if reply == nil {
			fmt.Println("[ProtoServer]Warning - Nil reply!")
		}
		antidote.SendProto(replyType, reply, conn)
	}
}

/*var defaultTopKProto *proto.ApbStaticReadObjectsResp
var defaultTopKMarshal []byte

func prepareTopKProtobuf() {
	scores := make([]crdt.TopKScore, tools.SharedConfig.GetIntConfig("topKSize", 100))
	selfRng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range scores {
		id, value, data := selfRng.Int31n(100000), selfRng.Int31n(100000), make([]byte, 0)
		scores[i] = crdt.TopKScore{Id: id, Score: value, Data: &data}
	}
	states := []crdt.State{crdt.TopKValueState{Scores: scores}}
	txnId := antidote.TransactionId(1)
	clk := clocksi.NewClockSiTimestamp()
	defaultTopKProto = antidote.CreateStaticReadResp(states, txnId, clk)
	defaultTopKMarshal, _ = pb.Marshal(defaultTopKProto)
}*/

//TODO: Error cases in which it should return ApbErrorResp
func handleStaticReadObjects(proto *proto.ApbStaticReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbStaticReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())

	objs := antidote.ProtoObjectsToAntidoteObjects(proto.GetObjects())
	replyChan := make(chan antidote.TMStaticReadReply)

	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp)
	return
}

func handleStaticRead(proto *proto.ApbStaticRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbStaticReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())

	objs := antidote.ProtoReadToAntidoteObjects(proto.GetFullreads(), proto.GetPartialreads())
	replyChan := make(chan antidote.TMStaticReadReply)

	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp)
	return
}

func handleStaticUpdateObjects(proto *proto.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())

	updates := antidote.ProtoUpdateOpToAntidoteUpdate(proto.GetUpdates())

	replyChan := make(chan antidote.TMStaticUpdateReply)

	tmChan <- createTMRequest(antidote.TMStaticUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)
	//TODO: Actually not ignore error
	ignore(reply.Err)

	respProto = antidote.CreateCommitOkResp(reply.TransactionId, reply.Timestamp)
	//fmt.Println(respProto.GetSuccess(), respProto.GetCommitTime(), respProto.GetErrorcode())
	return
}

func handleReadObjects(proto *proto.ApbReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	objs := antidote.ProtoObjectsToAntidoteObjects(proto.GetBoundobjects())
	replyChan := make(chan []crdt.State)

	tmChan <- createTMRequest(antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateReadObjectsResp(reply)
	return
}

func handleRead(proto *proto.ApbRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	objs := antidote.ProtoReadToAntidoteObjects(proto.GetFullreads(), proto.GetPartialreads())
	replyChan := make(chan []crdt.State)

	tmChan <- createTMRequest(antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateReadObjectsResp(reply)
	return
}

func handleUpdateObjects(proto *proto.ApbUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbOperationResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	updates := antidote.ProtoUpdateOpToAntidoteUpdate(proto.GetUpdates())

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

func handleStartTxn(proto *proto.ApbStartTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbStartTransactionResp) {

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

	respProto = antidote.CreateStartTransactionResp(reply.TransactionId, reply.Timestamp)
	return
}

func handleAbortTxn(proto *proto.ApbAbortTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	tmChan <- createTMRequest(antidote.TMAbortArgs{}, txnId, clientClock)

	respProto = antidote.CreateCommitOkResp(txnId, clientClock)
	//Returns a clock and success set as true. I assume the clock is the same as the one returned in startTxn?
	return
}

func handleCommitTxn(proto *proto.ApbCommitTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())
	replyChan := make(chan antidote.TMCommitReply)

	tmChan <- createTMRequest(antidote.TMCommitArgs{ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan

	//TODO: Errors?
	respProto = antidote.CreateCommitOkResp(txnId, reply.Timestamp)
	return
}

func handleNewTrigger(proto *proto.ApbNewTrigger, tmChan chan antidote.TransactionManagerRequest,
	clientId antidote.ClientId, ci antidote.CodingInfo) (respProto *proto.ApbNewTriggerReply) {

	replyChan := make(chan bool)

	tmChan <- createTMRequest(antidote.TMNewTriggerArgs{
		ReplyChan: replyChan,
		IsGeneric: proto.GetIsGeneric(),
		Source:    antidote.ProtoTriggerInfoToAntidote(proto.GetSource(), ci),
		Target:    antidote.ProtoTriggerInfoToAntidote(proto.GetTarget(), ci),
	}, 0, nil)

	<-replyChan

	//TODO: Errors?
	respProto = antidote.CreateNewTriggerReply()
	return
}

func handleGetTriggers(proto *proto.ApbGetTriggers, tmChan chan antidote.TransactionManagerRequest,
	clientId antidote.ClientId, ci antidote.CodingInfo) (respProto *proto.ApbGetTriggersReply) {

	replyChan := make(chan *antidote.TriggerDB)
	notifyChan := make(chan bool)

	tmChan <- createTMRequest(antidote.TMGetTriggersArgs{ReplyChan: replyChan, WaitFor: notifyChan}, 0, nil)

	triggerDB := <-replyChan

	triggerDB.DebugPrint("[PS]")
	//TODO: Errors?
	respProto = antidote.CreateGetTriggersReply(triggerDB, ci)
	notifyChan <- true
	return
}

func handleResetServer(tm *antidote.TransactionManager) (respProto *proto.ApbResetServerResp) {
	tm.ResetServer()
	fmt.Println("Forcing GB...")
	debug.FreeOSMemory()
	fmt.Println("Server successfully reset.")
	fmt.Println("Memory stats after reset:")
	printMemStats(&runtime.MemStats{}, 0)

	return &proto.ApbResetServerResp{}
}

func handleServerConn(tmChan chan antidote.TransactionManagerRequest, conn net.Conn) chan antidote.TMS2SReply {
	replyChan := make(chan antidote.TMS2SReply, 10)
	tmChan <- createTMRequest(antidote.TMServerConn{ReplyChan: replyChan}, 0, nil)

	go func() {
		var err error
		var msg pb.Message
		//fmt.Println("[PS]S2S receiver - ready")
		//Wait on replyChan forever, do a switch with received item, send back on connection
		for wrapper := range replyChan {
			//fmt.Println("[PS]S2S receiver - got reply (clientID, replyType)", wrapper.ClientID, wrapper.ReplyType)
			switch reply := wrapper.Reply.(type) {
			case antidote.TMStaticReadReply:
				msg = antidote.CreateStaticReadResp(reply.States, wrapper.TxnID, reply.Timestamp)
			case antidote.TMStartTxnReply:
				msg = antidote.CreateStartTransactionResp(wrapper.TxnID, reply.Timestamp)
			case []crdt.State:
				msg = antidote.CreateReadObjectsResp(reply)
			case antidote.TMUpdateReply:
				msg = antidote.CreateOperationResp()
			case antidote.TMStaticUpdateReply:
				msg = antidote.CreateCommitOkResp(wrapper.TxnID, reply.Timestamp)
			case antidote.TMCommitReply:
				msg = antidote.CreateCommitOkResp(wrapper.TxnID, reply.Timestamp)
			default:
				fmt.Printf("[PS]S2S unknown proto type (%d, %T, %v+)\n", wrapper.ReplyType, reply, reply)
			}
			//fmt.Println("[PS]S2S sending reply")
			err = antidote.SendProtoNoCheck(antidote.S2SReply, antidote.CreateS2SWrapperReplyProto(wrapper.ClientID, wrapper.ReplyType, msg), conn)
			//fmt.Println("[PS]S2S sent reply")
			if err != nil {
				fmt.Println("[PS]Error on S2S send:", nil)
				break
			}
		}
	}()

	return replyChan
}

func handleServerToServer(protobf *proto.S2SWrapper, tmChan chan antidote.TransactionManagerRequest, s2sChan chan antidote.TMS2SReply,
	conn net.Conn, tm *antidote.TransactionManager) {
	//"Just" send appropriate request
	var req antidote.TMRequestArgs
	var txnId antidote.TransactionId
	var clientClock clocksi.Timestamp
	var reply interface{}
	var replyType proto.WrapperType
	clientID := protobf.GetClientID()
	//fmt.Println("[PS]S2S request type:", *protobf.MsgID)
	switch *protobf.MsgID {
	case proto.WrapperType_STATIC_READ_OBJS:
		inProto, replyChan := protobf.StaticReadObjs, make(chan antidote.TMStaticReadReply)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
		objs := antidote.ProtoObjectsToAntidoteObjects(inProto.GetObjects())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_STATIC_READ_OBJS

	case proto.WrapperType_STATIC_READ:
		inProto, replyChan := protobf.StaticRead, make(chan antidote.TMStaticReadReply)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
		objs := antidote.ProtoReadToAntidoteObjects(inProto.GetFullreads(), inProto.GetPartialreads())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_STATIC_READ_OBJS

	case proto.WrapperType_STATIC_UPDATE:
		inProto, replyChan := protobf.StaticUpd, make(chan antidote.TMStaticUpdateReply)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
		updates := antidote.ProtoUpdateOpToAntidoteUpdate(inProto.GetUpdates())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_COMMIT

	case proto.WrapperType_START_TXN:
		inProto, replyChan := protobf.StartTxn, make(chan antidote.TMStartTxnReply)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTimestamp())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStartTxnArgs{ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_START_TXN

	case proto.WrapperType_READ_OBJS:
		inProto, replyChan := protobf.ReadObjs, make(chan []crdt.State)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
		objs := antidote.ProtoObjectsToAntidoteObjects(inProto.GetBoundobjects())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_READ_OBJS

	case proto.WrapperType_READ:
		inProto, replyChan := protobf.Read, make(chan []crdt.State)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
		objs := antidote.ProtoReadToAntidoteObjects(inProto.GetFullreads(), inProto.GetPartialreads())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_READ_OBJS

	case proto.WrapperType_UPD:
		inProto, replyChan := protobf.Upd, make(chan antidote.TMUpdateReply)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
		updates := antidote.ProtoUpdateOpToAntidoteUpdate(inProto.GetUpdates())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_UPD

	case proto.WrapperType_COMMIT:
		inProto, replyChan := protobf.CommitTxn, make(chan antidote.TMCommitReply)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMCommitArgs{ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_COMMIT

	case proto.WrapperType_ABORT:
		inProto := protobf.AbortTxn
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMAbortArgs{}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = antidote.TMCommitReply{Timestamp: clientClock}, proto.WrapperType_COMMIT
		//antidote.SendProto(antidote.CommitTransReply, antidote.CreateCommitOkResp(txnId, clientClock), conn)

	case proto.WrapperType_BC_PERMS_REQ:
		inProto := protobf.BcPermsReq
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.ProtoBCPermissionsReqToTM(inProto)}
		tmChan <- createTMRequest(req, 578902378, nil) //Random txID value
		return                                         //No reply

	default:
		fmt.Println("[PROTOSERVER]Error - Unknown type of S2S message:", protobf.MsgID)
		return
	}
	s2sChan <- antidote.TMS2SReply{ClientID: clientID, TxnID: 2, ReplyType: replyType, Reply: reply}
}

func createTMRequest(args antidote.TMRequestArgs, txnId antidote.TransactionId,
	clientClock clocksi.Timestamp) (request antidote.TransactionManagerRequest) {
	return antidote.TransactionManagerRequest{
		Args:          args,
		TransactionId: txnId,
		Timestamp:     clientClock,
	}
}

func startProfiling(configs *tools.ConfigLoader) {
	if profileCPUString, has := configs.GetAndHasConfig(CPU_PROFILE_KEY); has {
		profileCPU, _ = strconv.ParseBool(profileCPUString)
		if profileCPU {
			file, err := os.Create(configs.GetConfig(CPU_FILE_KEY))
			tools.CheckErr("Failed to create CPU profile file: ", err)
			pprof.StartCPUProfile(file)
			fmt.Println("Started CPU profiling")
		}
	}
	if profileMemString, has := configs.GetAndHasConfig(MEM_PROFILE_KEY); has {
		profileMem, _ = strconv.ParseBool(profileMemString)
		if profileMem {
			fmt.Println("Started mem profiling")
		}
	}
}

func stopProfiling(configs *tools.ConfigLoader) {
	if profileCPU || profileMem {
		sigs := make(chan os.Signal, 10)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigs
			fmt.Println("Saving profiles...")
			if profileCPU {
				pprof.StopCPUProfile()
			}
			if profileMem {
				file, err := os.Create(configs.GetConfig(MEM_FILE_KEY))
				defer file.Close()
				tools.CheckErr("Failed to create Memory profile file: ", err)
				pprof.WriteHeapProfile(file)
			}
			fmt.Println("Profiles saved, closing...")
			os.Exit(0)
		}()
	}
}

func loadConfigs() (configs *tools.ConfigLoader) {
	configFolder := flag.String("config", "default", "sub-folder in configs folder that contains the configuration files to be used.")
	rabbitMQIP := flag.String("rabbitMQIP", "F", "ip:port of this replica's rabbitMQ instance.")
	servers := flag.String("servers", "F", "list of ip:port of remote replicas' rabbitMQ instances, separated by spaces.")
	vhost := flag.String("rabbitVHost", "/crdts", "vhost to use with rabbitMQ.")
	port := flag.String("port", "F", "port for potionDB.")
	replicaID := flag.String("id", "F", "replicaID that uniquely identifies this replica.")
	doJoin := flag.String("doJoin", "F", "if this replica should query others about the current state before starting to accept client requests")
	stringBuckets := flag.String("buckets", "none", "list of buckets for the server to replicate.")
	disableRepl := flag.String("disableReplicator", "none", "if replicator should be disabled. False by default.")
	disableLog := flag.String("disableLog", "none", "if logging of operations should be disabled. False by default.")
	disableReadWaiting := flag.String("disableReadWaiting", "none", "if reads should wait until the materializer's clock is >= to the read's")
	useTC := flag.String("useTC", "none", "defines if traffic control should be applied to the connections."+
		"If true, the IPs and latencies must be defined in the configuration file.")
	tcIp := flag.String("tcIPs", "none", "defines the list of IPs for TC purposes. Must be actual IP addresses instead of aliases.")
	localPotionDBAddress := flag.String("selfIP", "none", "the ip:port of this replica. Used for administration purposes.")
	poolMax := flag.String("poolMax", "none", "max size of connections to each other server, for redirection requests purposes.")
	topKSize := flag.String("topKSize", "none", "number of entries that are considered 'on top' for a topK CRDT.")
	//Dataload flags
	doDataload := flag.String(DO_TPCH_DATALOAD, "none", "defines if the server should load some initial data.")
	sf := flag.String("scale", "none", "scale (SF) of tpch, if doing tpch dataload")
	//Check tpch configs for the location of this
	dataLoc := flag.String("dataLoc", "none", "if doing dataload, the location of the data")
	region := flag.String("region", "none", "if doing tpch dataload, the region that is associated to this server (1-n)")
	dummyDataSize := flag.String("initialMem", "none", "the size (bytes) of the initial block of data. This is used to avoid Go's GC to overcollect garbage and hinder system performance.")

	flag.Parse()
	configs = &tools.ConfigLoader{}
	//fmt.Println("Using config file:", *configFolder)
	configs.LoadConfigs(*configFolder)

	//If flags are present, override configs
	if isFlagValid(*rabbitMQIP, "F") {
		configs.ReplaceConfig("localRabbitMQAddress", *rabbitMQIP)
	}
	if isFlagValid(*servers, "F") {
		srv := *servers
		if srv[0] == '[' {
			srv = strings.Replace(srv[1:len(srv)-1], ",", " ", -1)
			fmt.Println(srv)
		}
		configs.ReplaceConfig("remoteRabbitMQAddresses", srv)
	}
	if isFlagValid(*vhost, "/crdts") {
		fmt.Println("Replacing vhost with", *vhost)
		configs.ReplaceConfig("rabbitVHost", *vhost)
	}
	if isFlagValid(*port, "F") {
		prt := *port
		if prt[0] == '[' {
			prt = strings.Replace(prt[1:len(prt)-1], ",", " ", -1)
		}
		configs.ReplaceConfig(PORT_KEY, prt)
		//configs.ReplaceConfig(PORT_KEY, *port)
	}
	if isFlagValid(*doJoin, "F") {
		configs.ReplaceConfig(DO_JOIN, *doJoin)
	}
	if isFlagValid(*replicaID, "F") {
		configs.ReplaceConfig("potionDBID", *replicaID)
	} else {
		/*
			//Get public address. Dial with UDP doesn't send anything by default, but it looks for
			//which network interface would be used to solve such request
			conn, err := net.Dial("udp", "8.8.8.8:80")
			var ip string
			if err != nil {
				log.Fatal(err)
				ip = strconv.FormatInt(rand.Int63(), 10)
			} else {
				defer conn.Close()
				ip = string(conn.LocalAddr().(*net.UDPAddr).IP)
			}
			configs.ReplaceConfig("potionDBID", strconv.FormatInt(int64(hashFunc.StringSum64(ip)), 10))
		*/
		configs.ReplaceConfig("potionDBID", strconv.FormatInt(rand.Int63(), 10))
	}
	if isFlagValid(*stringBuckets, "none") {
		bks := *stringBuckets
		if bks[0] == '[' {
			bks = strings.Replace(bks[1:len(bks)-1], ",", " ", -1)
			fmt.Println(bks)
		}
		configs.ReplaceConfig("buckets", bks)
	}
	if isFlagValid(*disableRepl, "none") {
		configs.ReplaceConfig("disableReplicator", *disableRepl)
	}
	if isFlagValid(*disableLog, "none") {
		configs.ReplaceConfig("disableLog", *disableRepl)
	}
	if isFlagValid(*disableReadWaiting, "none") {
		configs.ReplaceConfig("disableReadWaiting", *disableReadWaiting)
	}
	if isFlagValid(*useTC, "none") {
		configs.ReplaceConfig("useTC", *useTC)
	}
	if isFlagValid(*tcIp, "none") {
		ips := *tcIp
		if ips[0] == '[' {
			ips = strings.Replace(ips[1:len(ips)-1], ",", " ", -1)
			fmt.Println(ips)
		}
		configs.ReplaceConfig("tcIPs", ips)
	}
	if isFlagValid(*localPotionDBAddress, "none") {
		configs.ReplaceConfig("localPotionDBAddress", *localPotionDBAddress)
	}
	if isFlagValid(*poolMax, "none") {
		configs.ReplaceConfig("poolMax", *poolMax)
	}
	if isFlagValid(*topKSize, "none") {
		fmt.Println("[PS]TopKSize received from arguments:", *topKSize)
		configs.ReplaceConfig("topKSize", *topKSize)
	}
	if isFlagValid(*doDataload, "none") {
		configs.ReplaceConfig(DO_TPCH_DATALOAD, *doDataload)
	}
	if isFlagValid(*sf, "none") {
		configs.ReplaceConfig("scale", *sf)
	}
	if isFlagValid(*dataLoc, "none") {
		configs.ReplaceConfig("dataLoc", *dataLoc)
	}
	if isFlagValid(*region, "none") {
		configs.ReplaceConfig("region", *region)
	}
	if isFlagValid(*dummyDataSize, "none") {
		configs.ReplaceConfig("initialMem", *dummyDataSize)
	}
	fmt.Println(*doDataload)
	fmt.Println(*sf)
	fmt.Println(*dataLoc)
	fmt.Println(*region)
	shared.IsReplDisabled = configs.GetBoolConfig("disableReplicator", false)
	shared.IsLogDisabled = configs.GetBoolConfig("disableLog", false)
	shared.IsReadWaitingDisabled = configs.GetBoolConfig("disableReadWaiting", false)

	return
}

func isFlagValid(value string, diffThan ...string) bool {
	if value == "" {
		return false
	}
	for _, diff := range diffThan {
		if diff == value {
			return false
		}
	}
	return true
}

func handleTC(configs *tools.ConfigLoader) {
	if configs.GetBoolConfig("useTC", false) {
		tc := tools.MakeTcInfo(configs.GetStringSliceConfig("tcIPs", ""), configs.GetIntConfig("tcMyPos", 5),
			configs.GetStringSliceConfig("tcLatency", "10 10 10 10 10"))
		tc.FireTcCommands()
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

func notSupported(protobuf pb.Message) {
	fmt.Println("Received proto is recognized but not yet supported")
}

//Temporary method. This is used to avoid compile errors on unused variables
//This unused variables mark stuff that isn't being processed yet.
func ignore(any interface{}) {

}

func debugMemory(configs *tools.ConfigLoader) {
	shouldDebug, err := false, error(nil)
	if debugMem, has := configs.GetAndHasConfig(MEM_DEBUG); has {
		shouldDebug, err = strconv.ParseBool(debugMem)
	}
	if err != nil || !shouldDebug {
		return
	}

	memStats := runtime.MemStats{}
	var maxAlloc uint64 = 0
	//Go routine that pools memStats.Alloc frequently and stores the highest observed value
	go func() {
		for {
			currAlloc := memStats.Alloc
			if currAlloc > maxAlloc {
				maxAlloc = currAlloc
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	count := 0
	for {
		printMemStats(&memStats, maxAlloc)
		count++

		/*
			if count%4 == 0 {
				fmt.Println("Calling GC")
				runtime.GC()
			}
		*/

		time.Sleep(10000 * time.Millisecond)
	}
}

func printMemStats(memStats *runtime.MemStats, maxAlloc uint64) {
	runtime.ReadMemStats(memStats)
	const MB = 1048576
	fmt.Printf("Total mem stolen from OS: %d MB\n", memStats.Sys/MB)
	if maxAlloc != 0 {
		fmt.Printf("Max alloced: %d MB\n", maxAlloc/MB)
	}
	fmt.Printf("Currently alloced: %d MB\n", memStats.Alloc/MB)
	fmt.Printf("Mem that could be returned to OS: %d MB\n", (memStats.HeapIdle-memStats.HeapReleased)/MB)
	fmt.Printf("Number of objs still malloced: %d\n", memStats.HeapObjects)
	fmt.Printf("Largest heap size: %d MB\n", memStats.HeapSys/MB)
	fmt.Printf("Stack size stolen from OS: %d MB\n", memStats.StackSys/MB)
	fmt.Printf("Stack size in use: %d MB\n", memStats.StackInuse/MB)
	fmt.Printf("Number of goroutines: %d\n", runtime.NumGoroutine())
	fmt.Printf("Number of GC cycles: %d\n", memStats.NumGC)
	fmt.Println()
}
