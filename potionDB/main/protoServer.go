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
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"
	"tpch_data_processor/tpch"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	antidote "potionDB/potionDB/components"
	"potionDB/potionDB/utilities"
	"potionDB/shared/shared"

	"github.com/AndreRijo/go-tools/src/tools"

	"sqlToKeyValue/src/sql"

	//pb "github.com/golang/protobuf/proto"
	hashFunc "github.com/twmb/murmur3"
	pb "google.golang.org/protobuf/proto"
)

var (
	in               = bufio.NewReader(os.Stdin)
	profileCPU       bool
	profileMem       bool
	protobufTestMode int

	crdtTestMap        map[uint64]crdt.CRDT
	stateTestMap       map[uint64]crdt.State
	marshallTestMap    map[uint64][]byte
	testCRDT           crdt.CRDT
	testState          crdt.State
	marshalledTestData []byte

	start time.Time
)

func getCRDT(keyP crdt.KeyParams) crdt.CRDT {
	return crdtTestMap[hashFunc.StringSum64(keyP.Bucket+keyP.CrdtType.String()+keyP.Key)]
}

const (
	//Keys for configs
	PORT_KEY                     = "protoPort"
	MEM_DEBUG                    = "memDebug"
	DO_JOIN                      = "doJoin"
	DO_TPCH_DATALOAD             = "doDataload"
	CPU_PROFILE_KEY              = "withCPUProfile"
	MEM_PROFILE_KEY              = "withMemProfile"
	CPU_FILE_KEY                 = "cpuProfileFile"
	MEM_FILE_KEY                 = "memProfileFile"
	PROTO_TEST_CRDTMAP           = 1
	PROTO_TEST_STATEMAP          = 2
	PROTO_TEST_MARSHALLED_MAP    = 3
	PROTO_TEST_SINGLE_CRDT       = 4
	PROTO_TEST_SINGLE_STATE      = 5
	PROTO_TEST_SINGLE_MARSHALLED = 6
)

var trash []byte

func main() {
	start = time.Now()
	fmt.Printf("[PS]Started loading PotionDB at %s\n", start.Format("15:04:05.000"))
	//debug.SetGCPercent(-1)
	cancelChan := make(chan os.Signal, 1)
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	readyChan := make(chan bool, 1)
	go checkSigtermUntilStartupFinishes(cancelChan, readyChan)

	rand.Seed(time.Now().UTC().UnixNano())
	configs := loadConfigs()
	trash = make([]byte, configs.GetIntConfig("initialMem", 0))
	floatSize := float64(len(trash))
	fmt.Printf("[PS]Setting an initial empty array of size %.4f GB\n", floatSize/1000000000)
	startProfiling(configs)

	portString := configs.GetOrDefault(PORT_KEY, "8887")
	ports := strings.Split(portString, " ")
	shared.PotionDBPort, _ = strconv.Atoi(ports[0])
	ports = append(ports, strconv.Itoa(shared.PotionDBPort*4%65535)) //Special port for initial S2S.
	//tmpId, _ := strconv.ParseInt(portString, 0, 64)
	//tmpId2, _ := strconv.ParseInt(configs.GetConfig("potionDBID"), 10, 64)
	//id := int16((tmpId + tmpId2) % math.MaxInt16)
	tmpId, _ := strconv.ParseInt(configs.GetConfig("potionDBID"), 10, 64)
	id := int16(tmpId % (math.MaxInt16 * 2))
	shared.ReplicaID = id

	antidote.SetVMToUse()
	tm := antidote.Initialize(id)
	sqlP := antidote.InitializeSQLProcessor(tm)
	go handleTC(configs)
	time.Sleep(150 * time.Millisecond)

	doDataload := configs.GetBoolConfig(DO_TPCH_DATALOAD, false)
	fmt.Println(configs.GetConfig(DO_TPCH_DATALOAD))
	dp := antidote.DataloadParameters{}
	if doDataload {
		sf, dataLoc, region := configs.GetFloatConfig("scale", 1.0), configs.GetConfig("dataLoc"), int8(configs.GetIntConfig("region", -1))
		dp.Region, dp.Sf, dp.DataLoc, dp.Tm, dp.IsTMReady = region, sf, dataLoc, tm, make(chan bool, 1)
		doIndexload := configs.GetBoolConfig("doIndexload", false)
		if doIndexload {
			dp.IndexConfigs = tpch.IndexConfigs{
				IsGlobal: configs.GetBoolConfig("isGlobal", true), IndexFullData: configs.GetBoolConfig("indexFullData", true),
				UseTopKAll: configs.GetBoolConfig("useTopKAll", true), UseTopSum: configs.GetBoolConfig("useTopSum", true),
				//QueryNumbers: strings.Split(configs.GetOrDefault("queryNumbers", "3 5 11 14 15 18"), " "),
				QueryNumbers: strings.Split(configs.GetOrDefault("queryNumbers", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22"), " "),
			}
			fmt.Println("[PS]Doing dataload and indexload. QueryNumbers: ", dp.QueryNumbers)
			if protobufTestMode > 0 { //Pretend to be region 0 so that indexes still load
				dp.Region = 0
			}
		} else {
			fmt.Println("[PS]Doing dataload but not doing indexload.")
		}
		go antidote.LoadData(dp)
	} else {
		fmt.Println("[PS]Not doing dataload nor indexload.")
	}

	fmt.Println("[PS]ReplicaID:", id)
	connChan := make(chan net.Conn, 500)
	listenerChans := make([]chan bool, len(ports))

	time.Sleep(250 * time.Millisecond)
	//Start listeners early but only start processing once TM is ready
	//This is helpful to speed up the dataloading process and initial creation of S2S connections.
	for i, port := range ports {
		if i == len(ports)-1 {
			listenerChans[i] = make(chan bool, 1) //Unused for now but here to avoid nil exception when PotionDB gets ready.
			go startS2SListener(port, id, tm)
		} else {
			listenerChans[i] = make(chan bool, 1)
			go startListener(port, id, tm, connChan, listenerChans[i], sqlP)
		}
	}

	//Wait for joining mechanism, if it's enabled
	waitForTM(doDataload, configs.GetBoolConfig(DO_JOIN, true), tm, dp)

	for i, port := range ports {
		fmt.Println("[PS]PotionDB started at port", port, "with ReplicaID", id, "at", time.Now().Format("15:04:05.000"))
		listenerChans[i] <- true
	}

	checkDisabledComponents()

	/*if len(ports) > 1 {
		for _, port := range ports[1:] {
			go startListener(port, id, tm)
		}
	}*/
	go debugMemory(configs)
	stopProfiling(configs)

	//startListener(ports[0], id, tm)

	nConns, done := len(connChan), false
	for ; nConns < 0; nConns-- {
		conn := <-connChan
		go processConnection(conn, tm, sqlP, id)
	}
	timer := time.NewTimer(3 * time.Second)
	for !done { //Try again in case we receive some late connection attempt
		select {
		case conn := <-connChan:
			go processConnection(conn, tm, sqlP, id)
		case <-timer.C:
			done = true
		}
	}

	if protobufTestMode > 0 {
		time.Sleep(15 * time.Second)
		crdtTestMap, stateTestMap, marshallTestMap = make(map[uint64]crdt.CRDT), make(map[uint64]crdt.State), make(map[uint64][]byte)
		ic := antidote.InternalClient{}.Initialize(tm)
		//One CRDT per year + region.
		reads, i := make([]crdt.KeyParams, 25), 0
		//q5nr+Region_name+1993
		regions, years := []string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}, []string{"1993", "1994", "1995", "1996", "1997"}
		for _, region := range regions {
			base := "q5nr" + region
			for _, year := range years {
				reads[i] = crdt.KeyParams{Key: base + year, CrdtType: proto.CRDTType_RRMAP, Bucket: "INDEX"}
				i++
			}
		}
		crdts := ic.DoGetCRDTs(reads)
		txnId, clientClk := antidote.TransactionId(1), clocksi.NewClockSiTimestamp()
		for i, currCRDT := range crdts {
			crdtTestMap[getHash(reads[i])] = currCRDT
			state := currCRDT.Read(crdt.StateReadArguments{}, []crdt.UpdateArguments{})
			stateTestMap[getHash(reads[i])] = state
			marshallTestMap[getHash(reads[i])] = antidote.GetProtoMarshal(antidote.CreateStaticReadResp([]crdt.State{state}, txnId, clientClk))
		}
		firstKeyHash := getHash(reads[0])
		testCRDT, testState, marshalledTestData = crdtTestMap[firstKeyHash], stateTestMap[firstKeyHash], marshallTestMap[firstKeyHash]
		fmt.Println("[ProtoServer]Prepared CRDTs for testing in protoServer. Testing mode:", protobufTestMode)

	} /*else {
		fmt.Println("[ProtoServer]Not running protobuf benchmarking.")
	}*/

	/*go func() {
		time.Sleep(18 * time.Second)
		ic := antidote.InternalClient{}.Initialize(tm)
		region := int8(configs.GetIntConfig("region", -1))
		regions := []string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
		years := []string{"1993", "1994", "1995", "1996", "1997"}
		readP := make([]crdt.KeyParams, 5)
		for i, year := range years {
			key := "q5nr" + regions[region] + year
			keyP := crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: "INDEX"}
			readP[i] = keyP
		}

		crdts := ic.DoGetCRDTs(readP)
		for i, currCRDT := range crdts {
			state := currCRDT.Read(crdt.StateReadArguments{}, []crdt.UpdateArguments{})
			fmt.Printf("Initial state of %s: %v\n", "q5nr"+regions[region]+years[i], state)
		}

		time.Sleep(40 * time.Second)
		crdts = ic.DoGetCRDTs(readP)
		for i, currCRDT := range crdts {
			state := currCRDT.Read(crdt.StateReadArguments{}, []crdt.UpdateArguments{})
			fmt.Printf("Initial state of %s: %v\n", "q5nr"+regions[region]+years[i], state)
		}
	}()*/

	//Block so that the server does not close
	//select {}
	readyChan <- true //No longer need the other goroutine to look into cancelChan.
	fmt.Printf("[PS]Listening for shutdown signal at %s...\n", time.Now().String())
	sig := <-cancelChan
	fmt.Printf("[PS]Caught signal %v at %s: sending shut down signal to TM.\n", sig, time.Now().String())
	time.Sleep(500 * time.Millisecond)
	start := time.Now().UnixNano()
	tm.ShutDown()
	end := time.Now().UnixNano()
	diffMs := (end - start) / int64(time.Millisecond)
	if diffMs < 500 {
		fmt.Printf("[PS]TM sucessfully shut down in %d milliseconds. Waiting for half a second before shutting down PotionDB.\n", diffMs)
		time.Sleep(time.Duration(500-diffMs) * time.Millisecond)
	} else {
		fmt.Printf("[PS]TM successfully shut down.\n")
	}
	fmt.Printf("[PS]Shutting down PotionDB at %s.\n", time.Now().String())
}

// Listens to new connections on ports other than the main one while PotionDB isn't ready.
func listenBeforePotionDBStart(port string, id int16, tm *antidote.TransactionManager, sqlP *antidote.SQLProcessor, ready chan bool) {
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(port))
	utilities.CheckErr(utilities.PORT_ERROR, err)
	waitingConns := make([]net.Conn, 0, 10)
	tmReady := false
	for !tmReady {
		select {
		case tmReady = <-ready:
			for _, conn := range waitingConns {
				go processConnection(conn, tm, sqlP, id)
			}
		default:
			conn, err := server.Accept()
			utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
			waitingConns = append(waitingConns, conn)
		}
	}
	listenToConnections(server, port, id, tm)
}

func listenToConnections(server net.Listener, port string, id int16, tm *antidote.TransactionManager) {
	fmt.Println("PotionDB started at port", port, "with ReplicaID", id)
}

func startS2SListener(port string, id int16, tm *antidote.TransactionManager) {
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(port))
	utilities.CheckErr(utilities.PORT_ERROR, err)
	//Stop listening to port on shutdown
	defer server.Close()
	fmt.Printf("[PS]Started S2S-only listener at port %s at %s.\n", port, time.Now().Format("15:04:05.000"))

	for {
		//fmt.Printf("[PS]Waiting for S2S connection on port %s...\n", port)
		conn, err := server.Accept()
		//fmt.Printf("[PS]Accepted S2S connection on port %s...\n", port)
		utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
		go processS2SConnection(conn, tm, id)
	}
}

func startListener(port string, id int16, tm *antidote.TransactionManager, connChan chan net.Conn, listenerChan chan bool, sqlP *antidote.SQLProcessor) {
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(port))
	utilities.CheckErr(utilities.PORT_ERROR, err)
	//Stop listening to port on shutdown
	defer server.Close()
	ready := false
	fmt.Printf("[PS]Started listener at port %s at %s.\n", port, time.Now().Format("15:04:05.000"))

	for !ready {
		conn, err := server.Accept()
		utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
		select {
		case <-listenerChan:
			ready = true
			go processConnection(conn, tm, sqlP, id)
		default:
			connChan <- conn
		}
	}

	for {
		conn, err := server.Accept()
		utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
		go processConnection(conn, tm, sqlP, id)
	}
}

/*
func startListener(port string, id int16, tm *antidote.TransactionManager) {
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(port))

	utilities.CheckErr(utilities.PORT_ERROR, err)
	fmt.Println("PotionDB started at port", port, "with ReplicaID", id)

	//Stop listening to port on shutdown
	defer server.Close()

	for {
		conn, err := server.Accept()
		utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
		go processConnection(conn, tm, id)
	}
}
*/

func processS2SConnection(conn net.Conn, tm *antidote.TransactionManager, replicaID int16) {
	defer conn.Close()
	tmChan := tm.CreateClientHandler()
	var s2sChan chan antidote.TMS2SReply

	for {
		//fmt.Printf("[PS][S2SConnection]Waiting for proto,\n")
		protoType, protobuf, err := antidote.ReceiveProto(conn)
		//fmt.Printf("[PS][S2SConnection]Received proto %v\n", protoType)
		if err != nil {
			conn.Close()
			return
		}
		switch protoType {
		case antidote.ServerConnReplicaID:
			//fmt.Println("[PS][S2SConnection]Received ServerConnReplicaID")
			s2sChan = handleServerConnReplicaID(protobuf.(*proto.ApbServerConnReplicaID), tmChan, conn)
			//fmt.Println("[PS][S2SConnection]Finished processing ServerConnReplicaID")
		case antidote.S2S:
			handleServerToServer(protobuf.(*proto.S2SWrapper), tmChan, s2sChan, conn, tm)
		default:
			fmt.Println("[WARNING][PS][S2SConnection]Received unknown proto, ignored... sort of")
		}
	}
}

/*
Handles a connection initiated by a new client.
Connection protocol (for both client and server):
msgSize (int32), msgType (1 byte), protobuf
Note that this is the same interaction type as in antidote.

conn - the TCP connection between the client and this server.
*/
func processConnection(conn net.Conn, tm *antidote.TransactionManager, sqlP *antidote.SQLProcessor, replicaID int16) {
	utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Accepted connection.")
	defer conn.Close()
	tmChan := tm.CreateClientHandler()
	//TODO: Change this to a random ID generated inside the transaction. This ID should be different from transaction to transaction
	//The current solution can give problems in the Materializer when a commited transaction is put on hold and another transaction from the same client arrives
	var clientId antidote.ClientId = antidote.ClientId(rand.Uint64())
	clientCI := antidote.CodingInfo{}.Initialize()

	var replyType byte = 0
	var reply pb.Message = nil
	var s2sChan chan antidote.TMS2SReply = nil //Used if this is a server to server communication

	if protobufTestMode == PROTO_TEST_MARSHALLED_MAP || protobufTestMode == PROTO_TEST_SINGLE_MARSHALLED {
		for {
			protoType, protobuf, err := antidote.ReceiveProto(conn)
			if err != nil {
				conn.Close()
				return
			}
			switch protoType {
			case antidote.StaticReadObjs:
				pb := protobuf.(*proto.ApbStaticReadObjects)
				antidote.DecodeTxnDescriptor(pb.GetTransaction().GetTimestamp())
				objs := antidote.ProtoObjectsToAntidoteObjects(pb.GetObjects())
				if protobufTestMode == PROTO_TEST_MARSHALLED_MAP {
					conn.Write(marshallTestMap[getHash(objs[0].KeyParams)])
				} else {
					conn.Write(marshalledTestData)
				}
			case antidote.StaticRead:
				pb := protobuf.(*proto.ApbStaticRead)
				antidote.DecodeTxnDescriptor(pb.GetTransaction().GetTimestamp())
				objs := antidote.ProtoReadToAntidoteObjects(pb.GetFullreads(), pb.GetPartialreads())
				if protobufTestMode == PROTO_TEST_MARSHALLED_MAP {
					conn.Write(marshallTestMap[getHash(objs[0].KeyParams)])
				} else {
					conn.Write(marshalledTestData)
				}
			default:
				fmt.Println("[WARNING][ProtoServer]Only StaticReadObjs and StaticRead are supported when protobufTestMode is set to marshalled.")
			}
		}
	}

	for {
		//Read protobuf
		//utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Waiting for client's request...")
		protoType, protobuf, err := antidote.ReceiveProto(conn)
		//This works in MacOS, but not on windows. For now we'll add any error here
		//if err == io.EOF
		if err != nil {
			if err == io.EOF {
				/*
					date := time.Now().String()
					utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Connection closed by client.")
					fmt.Println("[ProtoServer]Connection closed by client, shutting down handler. Leaving connection open however at timestamp" + date)
				*/
				conn.Close()
				tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
			} else {
				conn.Close()
				date := time.Now().String()
				fmt.Printf("[ProtoServer]Error on reading proto from client, closing connection.. Type: %v, proto: %v, error: %s, time: %s\n", protoType, protobuf, err, date)
				//time.Sleep(3 * time.Second)
				//fmt.Println("[ProtoServer]Closing connection due to error at time", time.Now().String(), ".But for now, actually leaving connection open.")
				tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
			}
			return
		}
		utilities.CheckErr(utilities.NETWORK_READ_ERROR, err)

		switch protoType {
		case antidote.ReadObjs:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbReadObjects")
			replyType = antidote.ReadObjsReply
			reply = handleReadObjects(protobuf.(*proto.ApbReadObjects), tmChan, clientId)
		case antidote.Read:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbRead")
			replyType = antidote.ReadObjsReply
			reply = handleRead(protobuf.(*proto.ApbRead), tmChan, clientId)
		case antidote.UpdateObjs:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbUpdateObjects")
			replyType = antidote.OpReply
			reply = handleUpdateObjects(protobuf.(*proto.ApbUpdateObjects), tmChan, clientId)
		case antidote.StartTrans:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbStartTransaction")
			replyType = antidote.StartTransReply
			reply = handleStartTxn(protobuf.(*proto.ApbStartTransaction), tmChan, clientId)
		case antidote.AbortTrans:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbAbortTransaction")
			replyType = antidote.CommitTransReply
			reply = handleAbortTxn(protobuf.(*proto.ApbAbortTransaction), tmChan, clientId)
		case antidote.CommitTrans:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbCommitTransaction")
			replyType = antidote.CommitTransReply
			reply = handleCommitTxn(protobuf.(*proto.ApbCommitTransaction), tmChan, clientId)
		case antidote.StaticUpdateObjs:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbStaticUpdateObjects")
			replyType = antidote.CommitTransReply
			reply = handleStaticUpdateObjects(protobuf.(*proto.ApbStaticUpdateObjects), tmChan, clientId)
		case antidote.StaticReadObjs:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbStaticReadObjects")
			replyType = antidote.StaticReadObjsReply
			//antidote.SendProtoMarshal(replyType, defaultTopKMarshal, conn)
			//continue
			//reply = defaultTopKProto
			if protobufTestMode > 0 {
				reply = handleProtoTestRead(protobuf, protoType)
			} else {
				reply = handleStaticReadObjects(protobuf.(*proto.ApbStaticReadObjects), tmChan, clientId)
			}
		case antidote.StaticRead:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbStaticRead")
			replyType = antidote.StaticReadObjsReply
			//antidote.SendProtoMarshal(replyType, defaultTopKMarshal, conn)
			//continue
			//reply = defaultTopKProto
			if protobufTestMode > 0 {
				reply = handleProtoTestRead(protobuf, protoType)
			} else {
				reply = handleStaticRead(protobuf.(*proto.ApbStaticRead), tmChan, clientId)
			}
		case antidote.NewTrigger:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbNewTrigger")
			replyType = antidote.NewTriggerReply
			reply = handleNewTrigger(protobuf.(*proto.ApbNewTrigger), tmChan, clientId, clientCI)
		case antidote.GetTriggers:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbGetTriggers")
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
		case antidote.ServerConnReplicaID:
			s2sChan = handleServerConnReplicaID(protobuf.(*proto.ApbServerConnReplicaID), tmChan, conn)
			continue
		case antidote.SQLString:
			handleSQLString(protobuf.(*proto.ApbStringSQL), tmChan, sqlP, clientId)
			continue //TODO
		case antidote.SQLTyped:
			handleSQLTyped(protobuf.(*proto.ApbTypedSQL), tmChan, sqlP, clientId)
			continue //TODO
		case antidote.MultiConnect:
			nClients := int(*protobuf.(*proto.ApbMultiClientConnect).NClients)
			channels, replyChan := tm.UpgradeHandlerToMultiClient(tmChan, nClients)
			handleMultiClient(conn, channels, replyChan, nClients, clientId)
			return //If we ever get here, it means the connection was closed and we should just return gracefully.
		default:
			utilities.FancyErrPrint(utilities.PROTO_PRINT, replicaID, "Received unknown proto, ignored... sort of")
			fmt.Println("I don't know how to handle this proto", protoType)
		}
		utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Sending reply proto")
		if reply == nil {
			fmt.Println("[ProtoServer]Warning - Nil reply!")
		}
		//tsStart := time.Now().UnixNano()
		err = antidote.SendProtoNoCheck(replyType, reply, conn)
		if err != nil {
			conn.Close()
			fmt.Printf("[ProtoServer]Error on sending proto to client: %s. Closing connection.\n", err)
			return
		}
		//tsEnd := time.Now().UnixNano()
		//fmt.Printf("[PS]Protobuf sending took %d microseconds.\n", (tsEnd-tsStart)/int64(time.Duration(time.Microsecond)))
	}
}

func handleSQLString(proto *proto.ApbStringSQL, tmChan chan antidote.TransactionManagerRequest, sqlP *antidote.SQLProcessor, clientId antidote.ClientId) {
	sqlString := proto.GetSql()
	listener := sql.SQLStringToListener(sqlString)
	switch typedListener := listener.(type) {
	case *sql.ListenerCreateTable:
		ignore(typedListener)
	case *sql.ListenerCreateIndex:

	case *sql.MyViewSQLListener:

	case *sql.ListenerQuery:

	case *sql.ListenerInsert:

	case *sql.ListenerUpdate:

	case *sql.ListenerDelete:

	case *sql.ListenerDropTable:

	}
}

func handleSQLTyped(protobuf *proto.ApbTypedSQL, tmChan chan antidote.TransactionManagerRequest, sqlP *antidote.SQLProcessor, clientId antidote.ClientId) {
	switch protobuf.GetType() {
	case proto.SQL_Type_CREATE_TABLE:
		listener := sql.ListenerCreateTable{}.FromProtobuf(protobuf).(*sql.ListenerCreateTable)
		sqlP.ProcessCreateTable(listener)
	case proto.SQL_Type_CREATE_INDEX:

	case proto.SQL_Type_CREATE_VIEW:

	case proto.SQL_Type_INSERT:

	case proto.SQL_Type_UPDATE:

	case proto.SQL_Type_DELETE:

	case proto.SQL_Type_DROP:

	case proto.SQL_Type_QUERY:
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

func handleStaticReadObjects(proto *proto.ApbStaticReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbStaticReadObjectsResp) {

	replyChan, txnId := sendTMStaticReadObjectsRequest(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	return antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp)
}

func sendTMStaticReadObjectsRequest(proto *proto.ApbStaticReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan antidote.TMStaticReadReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())
	objs := antidote.ProtoObjectsToAntidoteObjects(proto.GetObjects())
	replyChan = make(chan antidote.TMStaticReadReply)
	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
}

func handleStaticRead(proto *proto.ApbStaticRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbStaticReadObjectsResp) {

	replyChan, txnId := sendTMStaticReadRequest(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	return antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp)
}

func sendTMStaticReadRequest(proto *proto.ApbStaticRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan antidote.TMStaticReadReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())
	objs := antidote.ProtoReadToAntidoteObjects(proto.GetFullreads(), proto.GetPartialreads())
	replyChan = make(chan antidote.TMStaticReadReply)
	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
}

func handleStaticUpdateObjects(proto *proto.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	replyChan, _ := sendTMStaticUpdateObjects(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	ignore(reply.Err)
	return antidote.CreateCommitOkResp(reply.TransactionId, reply.Timestamp)
}

func sendTMStaticUpdateObjects(proto *proto.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan antidote.TMStaticUpdateReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())
	updates := antidote.ProtoUpdateOpToAntidoteUpdate(proto.GetUpdates())
	replyChan = make(chan antidote.TMStaticUpdateReply)
	tmChan <- createTMRequest(antidote.TMStaticUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
}

func handleReadObjects(proto *proto.ApbReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbReadObjectsResp) {

	replyChan, _ := sendTMReadObjects(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	return antidote.CreateReadObjectsResp(reply)
}

func sendTMReadObjects(proto *proto.ApbReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan []crdt.State, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())
	objs := antidote.ProtoObjectsToAntidoteObjects(proto.GetBoundobjects())
	replyChan = make(chan []crdt.State)
	tmChan <- createTMRequest(antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
}

func handleRead(proto *proto.ApbRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbReadObjectsResp) {

	replyChan, _ := sendTMRead(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	return antidote.CreateReadObjectsResp(reply)
}

func sendTMRead(proto *proto.ApbRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan []crdt.State, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())
	objs := antidote.ProtoReadToAntidoteObjects(proto.GetFullreads(), proto.GetPartialreads())
	replyChan = make(chan []crdt.State)
	tmChan <- createTMRequest(antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
}

func handleUpdateObjects(proto *proto.ApbUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbOperationResp) {

	replyChan, _ := sendTMUpdateObjects(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	ignore(reply.Err)
	return antidote.CreateOperationResp()
}

func sendTMUpdateObjects(proto *proto.ApbUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan antidote.TMUpdateReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())
	updates := antidote.ProtoUpdateOpToAntidoteUpdate(proto.GetUpdates())
	replyChan = make(chan antidote.TMUpdateReply)
	tmChan <- createTMRequest(antidote.TMUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
}

func handleStartTxn(proto *proto.ApbStartTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbStartTransactionResp) {

	replyChan, _ := sendTMStartTxn(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	return antidote.CreateStartTransactionResp(reply.TransactionId, reply.Timestamp)
}

func sendTMStartTxn(proto *proto.ApbStartTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan antidote.TMStartTxnReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTimestamp())
	replyChan = make(chan antidote.TMStartTxnReply)
	tmChan <- createTMRequest(antidote.TMStartTxnArgs{ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
	//Examples of txn descriptors in antidote:
	//{tx_id,1550320956784892,<0.4144.0>}.
	//{tx_id,1550321073482453,<0.4143.0>}. (obtained on the op after the previous timestamp)
	//{tx_id,1550321245370469,<0.4146.0>}. (obtained after deleting the logs)
	//It's basically a timestamp plus some kind of counter?
}

func handleAbortTxn(proto *proto.ApbAbortTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	txnId, clientClock := sendTMAbortTxn(proto, tmChan, clientId)
	//Returns a clock and success set as true. I assume the clock is the same as the one returned in startTxn?
	return antidote.CreateCommitOkResp(txnId, clientClock)
}

func sendTMAbortTxn(proto *proto.ApbAbortTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (txnId antidote.TransactionId, clientClock clocksi.Timestamp) {

	txnId, clientClock = antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())
	tmChan <- createTMRequest(antidote.TMAbortArgs{}, txnId, clientClock)
	return txnId, clientClock
}

func handleCommitTxn(proto *proto.ApbCommitTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	replyChan, txnId := sendTMCommitTxn(proto, tmChan, clientId)
	reply := <-replyChan
	return antidote.CreateCommitOkResp(txnId, reply.Timestamp)
}

func sendTMCommitTxn(proto *proto.ApbCommitTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan antidote.TMCommitReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())
	replyChan = make(chan antidote.TMCommitReply)
	tmChan <- createTMRequest(antidote.TMCommitArgs{ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
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

func handleServerConnReplicaID(protobuf *proto.ApbServerConnReplicaID, tmChan chan antidote.TransactionManagerRequest, conn net.Conn) chan antidote.TMS2SReply {
	fmt.Printf("[PS]Got ServerConnReplicaID from %d at %s\n", protobuf.GetReplicaID(), time.Now().Format("15:04:05:000"))
	tmChan <- createTMRequest(antidote.TMReplicaID{ReplicaID: int16(protobuf.GetReplicaID()), IP: protobuf.GetMyIP(), Buckets: protobuf.GetMyBuckets()}, 0, nil)
	return handleServerConn(tmChan, conn)
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
		inProto, replyChan := protobf.StaticReadObjs, make(chan antidote.TMStaticReadReply, 1)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
		objs := antidote.ProtoObjectsToAntidoteObjects(inProto.GetObjects())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_STATIC_READ_OBJS

	case proto.WrapperType_STATIC_READ:
		inProto, replyChan := protobf.StaticRead, make(chan antidote.TMStaticReadReply, 1)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
		objs := antidote.ProtoReadToAntidoteObjects(inProto.GetFullreads(), inProto.GetPartialreads())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_STATIC_READ_OBJS

	case proto.WrapperType_STATIC_SINGLE_READ:
		inProto, replyChan := protobf.SingleRead, make(chan antidote.TMStaticReadReply, 1)
		obj := antidote.S2SSingleReadToAntidote(inProto)
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMSingleReadArgs{ReadParams: obj, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, 578902378, nil) //Random txnID value
		reply, replyType = <-replyChan, proto.WrapperType_STATIC_SINGLE_READ

	case proto.WrapperType_STATIC_UPDATE:
		inProto, replyChan := protobf.StaticUpd, make(chan antidote.TMStaticUpdateReply, 1)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
		updates := antidote.ProtoUpdateOpToAntidoteUpdate(inProto.GetUpdates())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_COMMIT

	case proto.WrapperType_START_TXN:
		inProto, replyChan := protobf.StartTxn, make(chan antidote.TMStartTxnReply, 1)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTimestamp())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStartTxnArgs{ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_START_TXN

	case proto.WrapperType_READ_OBJS:
		inProto, replyChan := protobf.ReadObjs, make(chan []crdt.State, 1)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
		objs := antidote.ProtoObjectsToAntidoteObjects(inProto.GetBoundobjects())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_READ_OBJS

	case proto.WrapperType_READ:
		inProto, replyChan := protobf.Read, make(chan []crdt.State, 1)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
		objs := antidote.ProtoReadToAntidoteObjects(inProto.GetFullreads(), inProto.GetPartialreads())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_READ_OBJS

	case proto.WrapperType_UPD:
		inProto, replyChan := protobf.Upd, make(chan antidote.TMUpdateReply, 1)
		txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
		updates := antidote.ProtoUpdateOpToAntidoteUpdate(inProto.GetUpdates())
		req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}}
		tmChan <- createTMRequest(req, txnId, clientClock)
		reply, replyType = <-replyChan, proto.WrapperType_UPD

	case proto.WrapperType_COMMIT:
		inProto, replyChan := protobf.CommitTxn, make(chan antidote.TMCommitReply, 1)
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

func handleMultiClient(conn net.Conn, tmChans []chan antidote.TransactionManagerRequest, replyChan chan antidote.TMMultiClientReply, nClients int, clientId antidote.ClientId) {
	go handleMultiClientReplies(conn, replyChan)
	clientIds := make([]antidote.ClientId, nClients)
	clientIds[0] = clientId
	for i := 1; i < nClients; i++ {
		clientIds[i] = antidote.ClientId(rand.Uint64())
	}

	var protobuf pb.Message
	var protoType byte
	var client uint16
	var err error

	fmt.Printf("[ProtoServer]Handling multi-client connection with %d clients\n", nClients)
	for {
		protoType, client, protobuf, err = antidote.ReceiveProtoMultiClient(conn)
		//fmt.Printf("[ProtoServer][MultiClient]Received proto of type %d from client %d\n", protoType, client)
		if err != nil {
			if err == io.EOF {
				conn.Close()
				for _, tmChan := range tmChans {
					tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
				}
				fmt.Printf("[ProtoServer]Client closed connection. Closing multi-client connection.\n")
			} else {
				conn.Close()
				date := time.Now().String()
				fmt.Printf("[ProtoServer]Error on reading proto from client, closing multi-client connection. Type: %v, proto: %v, error: %s, time: %s\n", protoType, protobuf, err, date)
				for _, tmChan := range tmChans {
					tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
				}
			}
			return
		}
		utilities.CheckErr(utilities.NETWORK_READ_ERROR, err)

		switch protoType {
		case antidote.ReadObjs:
			sendTMReadObjects(protobuf.(*proto.ApbReadObjects), tmChans[client], clientIds[client])
		case antidote.Read:
			sendTMRead(protobuf.(*proto.ApbRead), tmChans[client], clientIds[client])
		case antidote.UpdateObjs:
			sendTMUpdateObjects(protobuf.(*proto.ApbUpdateObjects), tmChans[client], clientIds[client])
		case antidote.StartTrans:
			sendTMStartTxn(protobuf.(*proto.ApbStartTransaction), tmChans[client], clientIds[client])
		case antidote.AbortTrans:
			sendTMAbortTxn(protobuf.(*proto.ApbAbortTransaction), tmChans[client], clientIds[client])
		case antidote.CommitTrans:
			sendTMCommitTxn(protobuf.(*proto.ApbCommitTransaction), tmChans[client], clientIds[client])
		case antidote.StaticUpdateObjs:
			sendTMStaticUpdateObjects(protobuf.(*proto.ApbStaticUpdateObjects), tmChans[client], clientIds[client])
		case antidote.StaticReadObjs:
			sendTMStaticReadObjectsRequest(protobuf.(*proto.ApbStaticReadObjects), tmChans[client], clientIds[client])
		case antidote.StaticRead:
			sendTMStaticReadRequest(protobuf.(*proto.ApbStaticRead), tmChans[client], clientIds[client])
		}
	}
}

// Extra goroutine that will listen to replyChan and send the replies to the clients of this multi-client connection.
func handleMultiClientReplies(conn net.Conn, replyChan chan antidote.TMMultiClientReply) {
	var respProto pb.Message
	var replyType byte
	var err error
	for {
		reply := <-replyChan
		//fmt.Printf("[ProtoServer][MultiClient]Got reply from TM: %v\n", reply)
		switch typedReply := reply.Reply.(type) {
		case antidote.TMStaticReadReply: //Replies to both static read and static read objects are the same
			respProto, replyType = antidote.CreateStaticReadResp(typedReply.States, reply.TxnId, typedReply.Timestamp), antidote.StaticReadObjsReply
		case antidote.TMStaticUpdateReply:
			respProto, replyType = antidote.CreateCommitOkResp(reply.TxnId, typedReply.Timestamp), antidote.CommitTransReply
		case []crdt.State: //Reply to both read and read objects are the same
			respProto, replyType = antidote.CreateReadObjectsResp(typedReply), antidote.ReadObjsReply
		case antidote.TMUpdateReply:
			respProto, replyType = antidote.CreateOperationResp(), antidote.OpReply
		case antidote.TMStartTxnReply:
			respProto, replyType = antidote.CreateStartTransactionResp(reply.TxnId, typedReply.Timestamp), antidote.StartTransReply
		case antidote.TMCommitReply:
			respProto, replyType = antidote.CreateCommitOkResp(reply.TxnId, typedReply.Timestamp), antidote.CommitTransReply

		}
		err = antidote.SendProtoMultiClientNoCheck(replyType, uint16(reply.ClientID), respProto, conn)
		if err != nil {
			conn.Close()
			fmt.Println("[ProtoServer]Error on sending proto to multi client:", err)
			return
		}
	}

}

func handleProtoTestRead(protobuf pb.Message, protoType byte) *proto.ApbStaticReadObjectsResp {
	var txnId antidote.TransactionId
	var clientClock clocksi.Timestamp
	var objs []crdt.ReadObjectParams

	if protoType == antidote.StaticReadObjs {
		typedProto := protobuf.(*proto.ApbStaticReadObjects)
		txnId, clientClock = antidote.DecodeTxnDescriptor(typedProto.GetTransaction().GetTimestamp())
		objs = antidote.ProtoObjectsToAntidoteObjects(typedProto.GetObjects())
	} else { //Static read
		typedProto := protobuf.(*proto.ApbStaticRead)
		txnId, clientClock = antidote.DecodeTxnDescriptor(typedProto.GetTransaction().GetTimestamp())
		objs = antidote.ProtoReadToAntidoteObjects(typedProto.GetFullreads(), typedProto.GetPartialreads())
	}

	states := make([]crdt.State, len(objs))
	if protobufTestMode == PROTO_TEST_CRDTMAP {
		for i := 0; i < len(objs); i++ {
			states[i] = crdtTestMap[getHash(objs[i].KeyParams)].Read(objs[i].ReadArgs, []crdt.UpdateArguments{})
		}
	} else if protobufTestMode == PROTO_TEST_STATEMAP {
		for i := 0; i < len(objs); i++ {
			states[i] = stateTestMap[getHash(objs[i].KeyParams)]
		}
	} else if protobufTestMode == PROTO_TEST_SINGLE_CRDT {
		for i := 0; i < len(objs); i++ {
			states[i] = testCRDT.Read(objs[i].ReadArgs, []crdt.UpdateArguments{})
		}
	} else { //protobufTestMode == PROTO_TEST_SINGLE_STATE
		for i := 0; i < len(objs); i++ {
			states[i] = testState
		}
	}
	return antidote.CreateStaticReadResp(states, txnId, clientClock)
	/*var currObj crdt.ReadObjectParams
	states := make([]crdt.State, len(objs))
	for i := 0; i < len(objs); i++ {
		currObj = objs[i]
		if strings.HasPrefix(currObj.Key, "q3") {
			state := crdt.TopKValueState{Scores: make([]crdt.TopKScore, 10)}
			for j := 0; j < len(state.Scores); j++ {
				data := []byte("2004-03-15_1-URGENT")
				state.Scores[j] = crdt.TopKScore{Id: int32(j * 100000), Score: int32(j * 10000), Data: &data}
			}
			states[i] = state
		} else if strings.HasPrefix(currObj.Key, "q5") {
			state := crdt.EmbMapEntryState{States: make(map[string]crdt.State, 5)}
			countries := []string{"INDONESIA", "INDIA", "FRANCE", "JAPAN", "AUSTRALIA"}
			for j := 0; j < 5; j++ {
				state.States[countries[j]] = crdt.CounterState{Value: int32(j * 100000)}
			}
			states[i] = state
		} else if strings.HasPrefix(currObj.Key, "q11") {
			topKState := crdt.TopKValueState{Scores: make([]crdt.TopKScore, 100)}
			for j := 0; j < 100; j++ {
				data := []byte{}
				topKState.Scores[j] = crdt.TopKScore{Id: int32(j * 1000), Score: int32(j * 100000), Data: &data}
			}
			states[i] = crdt.EmbMapEntryState{States: map[string]crdt.State{"q11sum": crdt.CounterState{Value: int32(i * 1000000)}, "q11iss": topKState}}
		} else if strings.HasPrefix(currObj.Key, "q14") {
			states[i] = crdt.AvgState{Value: float64(i) * 12.48}
		} else if strings.HasPrefix(currObj.Key, "q15") {
			states[i] = crdt.TopKValueState{Scores: []crdt.TopKScore{{Id: int32(i * 1000), Score: int32(i * 10000)}}}
		} else if strings.HasPrefix(currObj.Key, "q18") {
			state := crdt.TopKValueState{Scores: make([]crdt.TopKScore, 12)}
			for j := 0; j < 12; j++ {
				data := []byte("cust0000239_398283_2014-05-16_283928")
				state.Scores[j] = crdt.TopKScore{Id: int32(j * 100000), Score: int32(312 + j), Data: &data}
			}
			states[i] = state
		}
	}
	return antidote.CreateStaticReadResp(states, txnId, clientClock)*/
}

func createTMRequest(args antidote.TMRequestArgs, txnId antidote.TransactionId,
	clientClock clocksi.Timestamp) (request antidote.TransactionManagerRequest) {
	return antidote.TransactionManagerRequest{
		Args:          args,
		TransactionId: txnId,
		Timestamp:     clientClock,
	}
}

func waitForTM(doDataload, doJoin bool, tm *antidote.TransactionManager, dp antidote.DataloadParameters) {
	var currTime time.Time
	if doJoin {
		fmt.Println("[PS][JOIN]Joining existing servers, please stand by...")
		reply := tm.WaitUntilReady()
		if reply == antidote.TM_READY {
			if doDataload {
				dp.IsTMReady <- true
			}
			currTime = time.Now()
			fmt.Printf("[PS][JOIN]TM ready, replicaIDs known. Waiting for RabbitMQ. Current time: %s. Time since start: %dms.\n",
				currTime.Format("15:04:05.000"), (currTime.UnixNano()-start.UnixNano())/int64(time.Millisecond))
			reply = tm.WaitUntilReady()
			currTime = time.Now()
			fmt.Printf("[PS][JOIN]RabbitMQ ready, starting PotionDB at %s. Time since start: %dms.\n",
				currTime.Format("15:04:05.000"), (currTime.UnixNano()-start.UnixNano())/int64(time.Millisecond))
		} else {
			if doDataload {
				dp.IsTMReady <- true
			}
			currTime = time.Now()
			fmt.Printf("[PS][JOIN]TM and RabbitMQ ready, replicaIDs known. Starting PotionDB at %s. Time since start: %dms.\n",
				time.Now().Format("15:04:05.000"), (currTime.UnixNano()-start.UnixNano())/int64(time.Millisecond))
		}
	} else {
		fmt.Println("[PS][NOJOIN]Not doing join (known set of servers). Waiting for replicaIDs of existing replicas...")
		reply := tm.WaitUntilReady()
		if reply == antidote.TM_READY {
			if doDataload {
				dp.IsTMReady <- true
			}
			currTime = time.Now()
			fmt.Printf("[PS][NOJOIN]TM ready, replicaIDs known. Waiting for RabbitMQ. Current time: %s. Time since start: %dms.\n",
				currTime.Format("15:04:05.000"), (currTime.UnixNano()-start.UnixNano())/int64(time.Millisecond))
			reply = tm.WaitUntilReady()
			currTime = time.Now()
			fmt.Printf("[PS][NOJOIN]RabbitMQ ready, starting PotionDB at %s. Time since start: %dms.\n",
				time.Now().Format("15:04:05.000"), (currTime.UnixNano()-start.UnixNano())/int64(time.Millisecond))
		} else {
			if doDataload {
				dp.IsTMReady <- true
			}
			currTime = time.Now()
			fmt.Printf("[PS][NOJOIN]TM and RabbitMQ ready, replicaIDs known. Starting PotionDB at %s. Time since start: %dms.\n",
				time.Now().Format("15:04:05.000"), (currTime.UnixNano()-start.UnixNano())/int64(time.Millisecond))
		}
	}
}

func checkDisabledComponents() {
	if shared.IsCRDTDisabled {
		fmt.Println("[PS][WARNING]CRDTs are disabled - all CRDTs will be replaced with EmptyCRDTs. " +
			"This should only be used for debugging/specific performance analysis.")
	}
	if shared.IsLogDisabled {
		fmt.Println("[PS][WARNING]Logging is disabled - no records will be kept of the operations done. " +
			"This also means Replication will not work. This should only be used for debugging/specific performance analysis.")
	}
	if shared.IsReplDisabled {
		fmt.Println("[PS][WARNING]Replication is disabled - no updates will be sent or received to/from other replicas. " +
			"This should only be used for debugging/specific performance analysis.")
	}
}

func startProfiling(configs *tools.ConfigLoader) {
	if profileCPUString, has := configs.GetAndHasConfig(CPU_PROFILE_KEY); has {
		profileCPU, _ = strconv.ParseBool(profileCPUString)
		if profileCPU {
			file, err := os.Create(configs.GetConfig(CPU_FILE_KEY))
			utilities.CheckErr("Failed to create CPU profile file: ", err)
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
				utilities.CheckErr("Failed to create Memory profile file: ", err)
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
	region := flag.String("region", "none", "if doing tpch dataload, the region that is associated to this server (0-(n-1))")
	dummyDataSize := flag.String("initialMem", "none", "the size (bytes) of the initial block of data. This is used to avoid Go's GC to overcollect garbage and hinder system performance.")
	//Index flags
	doIndexload := flag.String("doIndexload", "none", "defines if the server should load the views of the initial data.")
	isGlobal := flag.String("isGlobal", "none", "if doing index load, true means the views are global; false creates views of only local (regional) data.")
	useTopKAll := flag.String("useTopKAll", "none", "if topKAddAll (resp topKRemoveAll) should be used when building views.")
	useTopSum := flag.String("useTopSum", "none", "for Q15 of TPC-H, if TopSum should be used instead of TopK.")
	indexFullData := flag.String("indexFullData", "none", "if optional data should be loaded in the views.")
	//queryNumbers := flag.String("queryNumbers", "none", "list of TPC-H queries to create views for. By default only views for queries Q3, Q5, Q11, Q14, Q15 and Q18 are loaded.")
	queryNumbers := flag.String("queryNumbers", "none", "list of TPC-H queries to create views for. By default views for all TPC-H queries are loaded.")
	protoTestMode := flag.String("protoTestMode", "none", "if true, queries return a default answer in order to evaluate protobuf's performance.")
	fastSingleRead := flag.String("fastSingleRead", "none", "if true, static reads for a single CRDT skip clock verification, thus avoiding a lock.")

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
		configs.ReplaceConfig("disableLog", *disableLog)
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
			//fmt.Println(ips)
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
		//fmt.Println("[PS]TopKSize received from arguments:", *topKSize)
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
	if isFlagValid(*doIndexload, "none") {
		configs.ReplaceConfig("doIndexload", *doIndexload)
	}
	if isFlagValid(*isGlobal, "none") {
		configs.ReplaceConfig("isGlobal", *isGlobal)
	}
	if isFlagValid(*useTopKAll, "none") {
		configs.ReplaceConfig("useTopKAll", *useTopKAll)
	}
	if isFlagValid(*useTopSum, "none") {
		configs.ReplaceConfig("useTopSum", *useTopSum)
	}
	if isFlagValid(*indexFullData, "none") {
		configs.ReplaceConfig("indexFullData", *indexFullData)
	}
	if isFlagValid(*queryNumbers, "none") {
		*queryNumbers = strings.Replace(*queryNumbers, ",", " ", -1)
		configs.ReplaceConfig("queryNumbers", *queryNumbers)
	}
	if isFlagValid(*protoTestMode, "none") {
		configs.ReplaceConfig("protoTestMode", *protoTestMode)
	}
	if isFlagValid(*fastSingleRead, "none") {
		configs.ReplaceConfig("fastSingleRead", *fastSingleRead)
	}
	fmt.Printf("[PS]DoDataload: %s; SF: %s; DataLoc: %s; Region: %s.\n", *doDataload, *sf, *dataLoc, *region)
	//fmt.Println(*doDataload)
	//fmt.Println(*sf)
	//fmt.Println(*dataLoc)
	//fmt.Println(*region)
	shared.IsReplDisabled = configs.GetBoolConfig("disableReplicator", false)
	shared.IsLogDisabled = configs.GetBoolConfig("disableLog", false)
	shared.IsReadWaitingDisabled = configs.GetBoolConfig("disableReadWaiting", false)
	protobufTestMode = configs.GetIntConfig("protoTestMode", 0)

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
		tc := utilities.MakeTcInfo(configs.GetStringSliceConfig("tcIPs", ""), configs.GetIntConfig("tcMyPos", 5),
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

// Temporary method. This is used to avoid compile errors on unused variables
// This unused variables mark stuff that isn't being processed yet.
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

func getHash(keyParams crdt.KeyParams) uint64 {
	return hashFunc.StringSum64(keyParams.Bucket + keyParams.CrdtType.String() + keyParams.Key)
}

func checkSigtermUntilStartupFinishes(cancelChan chan os.Signal, readyChan chan bool) {
	select {
	case <-cancelChan:
		fmt.Println("[PS]Received SIGTERM before startup finished. Shutting down forcefully.")
		os.Exit(1)
	case <-readyChan: //Nothing, just finish goroutine.

	}
}

/*func handleStaticReadObjects(proto *proto.ApbStaticReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbStaticReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())

	objs := antidote.ProtoObjectsToAntidoteObjects(proto.GetObjects())
	replyChan := make(chan antidote.TMStaticReadReply)

	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp)
	return
}*/

/*func handleStaticRead(proto *proto.ApbStaticRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbStaticReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())

	objs := antidote.ProtoReadToAntidoteObjects(proto.GetFullreads(), proto.GetPartialreads())
	replyChan := make(chan antidote.TMStaticReadReply)

	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	//tsStart := time.Now().UnixNano()
	respProto = antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp)
	//tsEnd := time.Now().UnixNano()
	//fmt.Printf("[PS]Protobuf generation took %d microseconds.\n", (tsEnd-tsStart)/int64(time.Duration(time.Microsecond)))
	return
}*/

/*func handleStaticUpdateObjects(proto *proto.ApbStaticUpdateObjects,
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
}*/

/*func handleReadObjects(proto *proto.ApbReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	objs := antidote.ProtoObjectsToAntidoteObjects(proto.GetBoundobjects())
	replyChan := make(chan []crdt.State)

	tmChan <- createTMRequest(antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateReadObjectsResp(reply)
	return
}*/

/*func handleRead(proto *proto.ApbRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbReadObjectsResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	objs := antidote.ProtoReadToAntidoteObjects(proto.GetFullreads(), proto.GetPartialreads())
	replyChan := make(chan []crdt.State)

	tmChan <- createTMRequest(antidote.TMReadArgs{ReadParams: objs, ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan
	close(replyChan)

	respProto = antidote.CreateReadObjectsResp(reply)
	return
}*/

/*func handleUpdateObjects(proto *proto.ApbUpdateObjects,
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
}*/

/*func handleStartTxn(proto *proto.ApbStartTransaction,
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
}*/

/*func handleAbortTxn(proto *proto.ApbAbortTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())

	tmChan <- createTMRequest(antidote.TMAbortArgs{}, txnId, clientClock)

	respProto = antidote.CreateCommitOkResp(txnId, clientClock)
	//Returns a clock and success set as true. I assume the clock is the same as the one returned in startTxn?
	return
}*/

/*func handleCommitTxn(proto *proto.ApbCommitTransaction,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransactionDescriptor())
	replyChan := make(chan antidote.TMCommitReply)

	tmChan <- createTMRequest(antidote.TMCommitArgs{ReplyChan: replyChan}, txnId, clientClock)

	reply := <-replyChan

	//TODO: Errors?
	respProto = antidote.CreateCommitOkResp(txnId, reply.Timestamp)
	return
}*/
