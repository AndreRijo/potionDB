package main

//https://opensource.com/article/18/5/building-concurrent-tcp-server-go
//https://golang.org/pkg/net/
//To run in vscode: ctrl+option+N. This *should* work when it actually updates $GOPATH
//From terminal: go to main folder and then: go run protoServer.go simpleClient.go

//profiling: https://github.com/google/pprof/blob/master/doc/README.md
//go tool pprof -http=localhost:36234 ../../profiles/8087/mem.prof

//TODO: Reuse of canals? Creating a new canal for each read/write seems like a waste... Should I ask the advisors?

import (
	"antidote"
	"bufio"
	"clocksi"
	"crdt"
	"flag"
	"fmt"
	rand "math/rand"
	"net"
	"os"
	"os/signal"
	"proto"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"
	"tools"

	pb "github.com/golang/protobuf/proto"
)

var (
	in         = bufio.NewReader(os.Stdin)
	profileCPU bool
	profileMem bool
)

const (
	//Keys for configs
	PORT_KEY        = "protoPort"
	MEM_DEBUG       = "memDebug"
	CPU_PROFILE_KEY = "withCPUProfile"
	MEM_PROFILE_KEY = "withMemProfile"
	CPU_FILE_KEY    = "cpuProfileFile"
	MEM_FILE_KEY    = "memProfileFile"
)

func main() {

	//debug.SetGCPercent(-1)

	configFolder := flag.String("config", "default", "sub-folder in configs folder that contains the configuration files to be used.")
	rabbitMQIP := flag.String("rabbitMQIP", "F", "ip:port of this replica's rabbitMQ instance.")
	servers := flag.String("servers", "F", "list of ip:port of remote replicas' rabbitMQ instances, separated by spaces.")

	flag.Parse()
	configs := &tools.ConfigLoader{}
	fmt.Println("Using config file:", *configFolder)
	configs.LoadConfigs(*configFolder)

	//If flags are present, override configs
	if *rabbitMQIP != "F" && *rabbitMQIP != "" {
		configs.ReplaceConfig("localRabbitMQAddress", *rabbitMQIP)
	}
	if *servers != "F" && *servers != "" {
		srv := *servers
		if srv[0] == '[' {
			srv = strings.Replace(srv[1:len(srv)-1], ",", " ", -1)
			fmt.Println(srv)
		}
		configs.ReplaceConfig("remoteRabbitMQAddresses", srv)
	}

	startProfiling(configs)

	portString := configs.GetConfig(PORT_KEY)
	rand.Seed(time.Now().UTC().UnixNano())
	tmpId, _ := strconv.ParseInt(portString, 0, 64)
	id := int16(tmpId)

	tm := antidote.Initialize(id)
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(portString))

	tools.CheckErr(tools.PORT_ERROR, err)
	fmt.Println("PotionDB started at port", portString)

	//Stop listening to port on shutdown
	defer server.Close()

	go debugMemory(configs)
	stopProfiling(configs)

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

	var replyType byte = 0
	var reply pb.Message = nil
	for {
		//TODO: Handle when client breaks connection or sends invalid data
		//Possible invalid data case (e.g.): sends code for "StaticUpdateObjs" but instead sends a protobuf of another type (e.g: StartTrans)
		//Read protobuf
		tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Waiting for client's request...")
		protoType, protobuf, err := antidote.ReceiveProto(conn)
		//This works in MacOS, but not on windows. For now we'll add any error here
		//if err == io.EOF
		if err != nil {
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Connection closed by client.")
			tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
			conn.Close()
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
			reply = handleStaticReadObjects(protobuf.(*proto.ApbStaticReadObjects), tmChan, clientId)
		case antidote.StaticRead:
			tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Received proto of type ApbStaticRead")
			replyType = antidote.StaticReadObjsReply
			reply = handleStaticRead(protobuf.(*proto.ApbStaticRead), tmChan, clientId)
		default:
			tools.FancyErrPrint(tools.PROTO_PRINT, replicaID, "Received unknown proto, ignored... sort of")
			fmt.Println("I don't know how to handle this proto", protoType)
		}
		tools.FancyDebugPrint(tools.PROTO_PRINT, replicaID, "Sending reply proto")
		antidote.SendProto(replyType, reply, conn)
	}
}

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

	const MB = 1048576
	count := 0
	for {
		runtime.ReadMemStats(&memStats)
		fmt.Printf("Total mem stolen from OS: %d MB\n", memStats.Sys/MB)
		fmt.Printf("Max alloced: %d MB\n", maxAlloc/MB)
		fmt.Printf("Currently alloced: %d MB\n", memStats.Alloc/MB)
		fmt.Printf("Mem that could be returned to OS: %d MB\n", (memStats.HeapIdle-memStats.HeapReleased)/MB)
		fmt.Printf("Number of objs still malloced: %d\n", memStats.HeapObjects)
		fmt.Printf("Largest heap size: %d MB\n", memStats.HeapSys/MB)
		fmt.Printf("Stack size stolen from OS: %d MB\n", memStats.StackSys/MB)
		fmt.Printf("Stack size in use: %d MB\n", memStats.StackInuse/MB)
		fmt.Printf("Number of goroutines: %d\n", runtime.NumGoroutine())
		fmt.Printf("Number of GC cycles: %d\n", memStats.NumGC)
		fmt.Println()
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
