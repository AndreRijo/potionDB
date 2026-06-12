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
	profileDur       int
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
	MEM_DEBUG_PERIOD             = "memDebugPeriod"
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
	START_CLIENT_BUF_SIZE        = 1024 //1KB.

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
	//go forceGC()

	currTs := time.Now().UTC().UnixNano()
	rand.Seed(currTs)
	rng := rand.New(rand.NewSource(currTs))
	configs := loadConfigs()
	trash = make([]byte, configs.GetIntConfig("initialMem", 0)) //TODO: Maybe can just clear this after a short while.
	floatSize := float64(len(trash))
	fmt.Printf("[PS]Setting an initial empty array of size %.4f GB\n", floatSize/1000000000)
	startProfiling(configs)
	stopProfiling(configs)
	go debugMemory(configs)

	portString := configs.GetOrDefault(PORT_KEY, "8887")
	ports := strings.Split(portString, " ")
	shared.PotionDBPort, _ = strconv.Atoi(ports[0])
	ports = append(ports, strconv.Itoa(shared.PotionDBPort*4%65535)) //Special port for initial S2S.
	//tmpId, _ := strconv.ParseInt(portString, 0, 64)
	//tmpId2, _ := strconv.ParseInt(configs.GetConfig("potionDBID"), 10, 64)
	//id := int16((tmpId + tmpId2) % math.MaxInt16)
	tmpId, _ := strconv.ParseInt(configs.GetConfig("potionDBID"), 10, 64)
	id := uint16(tmpId % math.MaxUint16)
	shared.ReplicaID = id

	antidote.SetVMToUse()
	doDataload := configs.GetBoolConfig(DO_TPCH_DATALOAD, false)
	fmt.Println(configs.GetConfig(DO_TPCH_DATALOAD))
	tm := antidote.Initialize(id, doDataload)
	sqlP := antidote.InitializeSQLProcessor(tm)
	go handleTC(configs)
	//time.Sleep(150 * time.Millisecond) //Should no longer be necessary.

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
				QueryNumbers:  strings.Split(configs.GetOrDefault("queryNumbers", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22"), " "),
				DoesIndexLoad: doIndexload,
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

	//time.Sleep(250 * time.Millisecond)
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
	//stopProfiling(configs)

	//startListener(ports[0], id, tm)
	txnDescSize := tm.GetClkByteSize() + 8 //+8 for the txnId in the descriptor

	nConns, done := len(connChan), false
	for ; nConns < 0; nConns-- {
		conn := <-connChan
		go processConnection(conn, tm, sqlP, id, txnDescSize, antidote.ClientId(rng.Uint64()))
	}
	timer := time.NewTimer(3 * time.Second)
	for !done { //Try again in case we receive some late connection attempt
		select {
		case conn := <-connChan:
			go processConnection(conn, tm, sqlP, id, txnDescSize, antidote.ClientId(rng.Uint64()))
		case <-timer.C:
			done = true
		}
	}

	if protobufTestMode > 0 {
		time.Sleep(15 * time.Second)
		stateBuf := crdt.NewBufsToReturn()
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
		txnId, clientClk := antidote.TransactionId(1), clocksi.NewSliceTimestamp()
		for i, currCRDT := range crdts {
			crdtTestMap[getHash(reads[i])] = currCRDT
			state := currCRDT.Read(crdt.StateReadArguments{}, []crdt.UpdateArguments{})
			stateTestMap[getHash(reads[i])] = state
			marshallTestMap[getHash(reads[i])] = antidote.GetProtoMarshal(antidote.CreateStaticReadResp([]crdt.State{state}, txnId, clientClk, stateBuf))
			stateBuf.ReturnBufs()
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
	trash = nil       //We no longer need this block of memory, it already served its purpose.
	fmt.Printf("[PS]Listening for shutdown signal at %s...\n", time.Now().String())
	sig := <-cancelChan
	fmt.Printf("[PS]Caught signal %v at %s: sending shut down signal to TM.\n", sig, time.Now().String())
	time.Sleep(200 * time.Millisecond)
	start := time.Now().UnixNano()
	shutDownChan := make(chan struct{}, 1)
	go func() {
		tm.ShutDown()
		shutDownChan <- struct{}{}
	}()
	select {
	case <-shutDownChan:

	case <-time.After(2 * time.Second):
		fmt.Printf("[PS]Internals did not shut down in time, a connection may be stuck. Forcing shutdown.\n")
		os.Exit(1)
	}
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
func listenBeforePotionDBStart(port string, id uint16, tm *antidote.TransactionManager, sqlP *antidote.SQLProcessor, ready chan bool) {
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(port))
	utilities.CheckErr(utilities.PORT_ERROR, err)
	waitingConns := make([]net.Conn, 0, 10)
	tmReady := false
	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	for !tmReady {
		select {
		case tmReady = <-ready:
			txnDescSize := tm.GetClkByteSize() + 8 //+8 for the txnId in the descriptor
			for _, conn := range waitingConns {
				go processConnection(conn, tm, sqlP, id, txnDescSize, antidote.ClientId(rng.Uint64()))
			}
		default:
			conn, err := server.Accept()
			utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
			waitingConns = append(waitingConns, conn)
		}
	}
	listenToConnections(server, port, id, tm)
}

func listenToConnections(server net.Listener, port string, id uint16, tm *antidote.TransactionManager) {
	fmt.Println("PotionDB started at port", port, "with ReplicaID", id)
}

func startS2SListener(port string, id uint16, tm *antidote.TransactionManager) {
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(port))
	utilities.CheckErr(utilities.PORT_ERROR, err)
	//Stop listening to port on shutdown
	defer server.Close()
	fmt.Printf("[PS]Started S2S-only listener at port %s at %s.\n", port, time.Now().Format("15:04:05.000"))
	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for {
		//fmt.Printf("[PS]Waiting for S2S connection on port %s...\n", port)
		conn, err := server.Accept()
		//fmt.Printf("[PS]Accepted S2S connection on port %s...\n", port)
		utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
		go processS2SConnection(conn, tm, id, antidote.ClientId(rng.Uint64()))
	}
}

func startListener(port string, id uint16, tm *antidote.TransactionManager, connChan chan net.Conn, listenerChan chan bool, sqlP *antidote.SQLProcessor) {
	server, err := net.Listen("tcp", "0.0.0.0:"+strings.TrimSpace(port))
	utilities.CheckErr(utilities.PORT_ERROR, err)
	//Stop listening to port on shutdown
	defer server.Close()
	ready := false
	fmt.Printf("[PS]Started listener at port %s at %s.\n", port, time.Now().Format("15:04:05.000"))
	var txnDescSize int
	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for !ready {
		conn, err := server.Accept()
		utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
		select {
		case <-listenerChan:
			ready, txnDescSize = true, tm.GetClkByteSize()+8 //+8 for the txnId in the descriptor
			go processConnection(conn, tm, sqlP, id, txnDescSize, antidote.ClientId(rng.Uint64()))
		default:
			connChan <- conn
		}
	}

	for {
		conn, err := server.Accept()
		utilities.CheckErr(utilities.NEW_CONN_ERROR, err)
		go processConnection(conn, tm, sqlP, id, txnDescSize, antidote.ClientId(rng.Uint64()))
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

/*func processS2SConnection(conn net.Conn, tm *antidote.TransactionManager, replicaID uint16) {
	defer conn.Close()
	tmChan := tm.CreateClientHandler(antidote.TM_SERVER_CLIENT)
	var s2sChan chan antidote.TMS2SReply
	buf := make([]byte, START_CLIENT_BUF_SIZE)
	var protoType byte
	var protobuf pb.Message
	var err error
	txnDescSize := -1
	var clientBufs *antidote.ClientBuffers
	stateBuf := crdt.NewBufsToReturn()

	for {
		//fmt.Printf("[PS][S2SConnection]Waiting for proto,\n")
		//protoType, protobuf, err := antidote.ReceiveProto(conn)
		protoType, protobuf, err, buf = antidote.ReceiveProtoReusableBufferVT(conn, buf)
		//fmt.Printf("[PS][S2SConnection]Received proto %v\n", protoType)
		if err != nil {
			conn.Close()
			return
		}
		switch protoType {
		case antidote.ServerConnReplicaID:
			//fmt.Println("[PS][S2SConnection]Received ServerConnReplicaID")
			s2sChan = handleServerConnReplicaID(protobuf.(*proto.ApbServerConnReplicaID), tmChan, conn, stateBuf, clientBufs)
			//fmt.Println("[PS][S2SConnection]Finished processing ServerConnReplicaID")
		case antidote.S2S:
			if txnDescSize == -1 {
				txnDescSize = tm.GetClkByteSize() + 8 //+8 for the txnId in the descriptor
				clientBufs = antidote.InitializeClientBuffers(txnDescSize)
			}
			handleServerToServer(protobuf.(*proto.S2SWrapper), tmChan, s2sChan, conn, tm, stateBuf, clientBufs)
		default:
			fmt.Println("[WARNING][PS][S2SConnection]Received unknown proto, ignored... sort of")
		}
	}
}*/

func processS2SConnection(conn net.Conn, tm *antidote.TransactionManager, replicaID uint16, clientId antidote.ClientId) {
	reqChan, replyChan := tm.CreateClientS2SHandler()
	var protoType byte
	var protobuf pb.Message
	var err error
	inBuf, outBuf := make([]byte, START_CLIENT_BUF_SIZE), make([]byte, START_CLIENT_BUF_SIZE)
	stateBuf := crdt.NewBufsToReturn()

	protoType, protobuf, err, inBuf = antidote.ReceiveProtoReusableBufferVT(conn, inBuf)
	if err != nil {
		fmt.Printf("[PS][processS2SConnection]Error on receiving early S2S connection - maybe the sender server crashed? Error: %v. Closing connection.\n", err)
		conn.Close()
		return
	}
	txnDescSize := tm.GetClkByteSize() + 8
	if txnDescSize == 8 { //Most likely the clock size is unknown at this point, as we're discovering the existing replicas. Set buffer to a "big enough" size.
		txnDescSize = 200
	}
	clientBuf := antidote.InitializeClientBuffers(txnDescSize)
	*clientBuf.PbBuffers = proto.PbBuffers{}
	clientBuf.S2SInit()
	handleS2SConn(conn, reqChan, replyChan, clientId, protoType, protobuf, inBuf, outBuf, stateBuf, clientBuf)
}

/*
Handles a connection initiated by a new client.
Connection protocol (for both client and server):
msgSize (int32), msgType (1 byte), protobuf
Note that this is the same interaction type as in antidote.

conn - the TCP connection between the client and this server.
*/
func processConnection(conn net.Conn, tm *antidote.TransactionManager, sqlP *antidote.SQLProcessor, replicaID uint16, txnDescSize int, clientId antidote.ClientId) {
	utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Accepted connection.")
	defer conn.Close()
	tmChan := tm.CreateClientHandler()
	clientCI := antidote.CodingInfo{}.Initialize()

	var replyType, protoType byte = 0, 0
	var reply, protobuf pb.Message = nil, nil
	//var s2sChan chan antidote.TMS2SReply = nil //Used if this is a server to server communication
	var err error
	isS2SConn := false //If we detect this to be a S2S (Server-To-Server) conn, we will later lock into methods that only handle S2S requests.

	//Two buffers are needed, as some reads may re-use the reads' data as part of the reply. Namely with unsafe.
	inBuf, outBuf := make([]byte, START_CLIENT_BUF_SIZE), make([]byte, START_CLIENT_BUF_SIZE) //Buffers for reading from and writing to the client. Re-usable to avoid recurrent allocs and GC pressure.
	ignore(outBuf)
	//buf := make([]byte, START_CLIENT_BUF_SIZE)         //Re-usable buffer used by both ReceiveProtoReusableBuffer and SendProtoReusable buffer, to avoid recurrent allocs and GC pressure for data sending/receiving
	stateBuf := crdt.NewBufsToReturn()                          //Collects large buffers that are used by CRDT states. These buffers should be returned after the states are Marshalled (or after sent to the client).
	clientBufs := antidote.InitializeClientBuffers(txnDescSize) //Declaring some re-usable reply protobufs, to reduce GC pressure.

	if protobufTestMode == PROTO_TEST_MARSHALLED_MAP || protobufTestMode == PROTO_TEST_SINGLE_MARSHALLED {
		for {
			//protoType, protobuf, err := antidote.ReceiveProto(conn)
			protoType, protobuf, err, inBuf = antidote.ReceiveProtoReusableBufferVT(conn, inBuf)
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

	//queryResult := make([]pb.Message, 23) //TODO: Temporary, remove.
	//queryResult := make([][]byte, 23)
	//queryResult := make([]tools.Triple[[]crdt.State, antidote.TransactionId, clocksi.Timestamp], 23)
	//var stateReply tools.Triple[[]crdt.State, antidote.TransactionId, clocksi.Timestamp] //Temporary.
	//nQueryResult := 0
	//var wasNil bool
	//targetQuery := 23

	for {
		//Read protobuf
		//utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Waiting for client's request...")
		//TODO: UNDO.
		//protoType, protobuf, err := antidote.ReceiveProto(conn)
		//protoType, protobuf, err, inBuf = antidote.ReceiveProtoReusableBufferVT(conn, inBuf)
		//protoType, protobuf, err := antidote.ReceiveProtoVT(conn)
		//inBuf = make([]byte, START_CLIENT_BUF_SIZE) //TODO: Remove.
		protoType, protobuf, err, inBuf = antidote.ReceiveProtoReusableBufferVTClientBuf(conn, inBuf, clientBufs)
		//TODO: Remove, only for debugging.
		start := time.Now().UnixNano()
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

		//if nQueryResult < targetQuery {
		switch protoType {
		case antidote.ReadObjs:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbReadObjects")
			replyType = antidote.ReadObjsReply
			reply = handleReadObjects(protobuf.(*proto.ApbReadObjects), tmChan, clientId, stateBuf)
		case antidote.Read:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbRead")
			replyType = antidote.ReadObjsReply
			reply = handleRead(protobuf.(*proto.ApbRead), tmChan, clientId, stateBuf)
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
			reply = handleStaticUpdateObjects(protobuf.(*proto.ApbStaticUpdateObjects), tmChan, clientId, clientBufs)
		case antidote.StaticReadObjs:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbStaticReadObjects")
			replyType = antidote.StaticReadObjsReply
			//antidote.SendProtoMarshal(replyType, defaultTopKMarshal, conn)
			//continue
			//reply = defaultTopKProto
			//if nQueryResult < targetQuery {
			if protobufTestMode > 0 {
				reply = handleProtoTestRead(protobuf, protoType, stateBuf)
			} else {
				reply = handleStaticReadObjects(protobuf.(*proto.ApbStaticReadObjects), tmChan, clientId, stateBuf, clientBufs)
				//stateReply = handleStaticReadObjectsDebug(protobuf.(*proto.ApbStaticReadObjects), tmChan, clientId)
			}
			//}
		case antidote.StaticRead:
			utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Received proto of type ApbStaticRead")
			replyType = antidote.StaticReadObjsReply
			//antidote.SendProtoMarshal(replyType, defaultTopKMarshal, conn)
			//continue
			//reply = defaultTopKProto
			//if nQueryResult < targetQuery {
			if protobufTestMode > 0 {
				reply = handleProtoTestRead(protobuf, protoType, stateBuf)
			} else {
				reply = handleStaticRead(protobuf.(*proto.ApbStaticRead), tmChan, clientId, stateBuf, clientBufs)
				//stateReply = handleStaticReadDebug(protobuf.(*proto.ApbStaticRead), tmChan, clientId)
			}
			//}
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
		/*case antidote.ServerConn:
			s2sChan = handleServerConn(tmChan, conn, stateBuf, clientBufs)
			continue
		case antidote.ServerConnReplicaID:
			s2sChan = handleServerConnReplicaID(protobuf.(*proto.ApbServerConnReplicaID), tmChan, conn, stateBuf, clientBufs)
			continue*/
		case antidote.ServerConn, antidote.ServerConnReplicaID:
			//From now on, the connection will be treated as S2S-only and managed in handleNewS2SConn.
			upgradeToS2SConn(conn, tm, tmChan, clientId, protoType, protobuf, inBuf, outBuf, stateBuf, clientBufs)
			isS2SConn = true
			//If we ever break out from there, it means the connection was closed. We jump to the finallizer.
			goto S2SFinallizer
		//No longer possible here.
		/*case antidote.S2S:
		handleServerToServer(protobuf.(*proto.S2SWrapper), tmChan, s2sChan, conn, tm, stateBuf, clientBufs)
		continue*/
		case antidote.SQLString:
			handleSQLString(protobuf.(*proto.ApbStringSQL), tmChan, sqlP, clientId)
			continue //TODO
		case antidote.SQLTyped:
			handleSQLTyped(protobuf.(*proto.ApbTypedSQL), tmChan, sqlP, clientId)
			continue //TODO
		case antidote.MultiConnect:
			nClients := int(*protobuf.(*proto.ApbMultiClientConnect).NClients)
			channels, replyChan := tm.UpgradeHandlerToMultiClient(tmChan, nClients)
			handleMultiClient(conn, channels, replyChan, nClients, clientId, txnDescSize)
			return //If we ever get here, it means the connection was closed and we should just return gracefully.
		default:
			utilities.FancyErrPrint(utilities.PROTO_PRINT, replicaID, "Received unknown proto, ignored... sort of")
			fmt.Println("[PS]I don't know how to handle this proto", protoType)
			panic("[PS]Unknown proto type received. Code: " + strconv.Itoa(int(protoType)))
		}
		//utilities.FancyDebugPrint(utilities.PROTO_PRINT, replicaID, "Sending reply proto")
		//}
		//tsStart := time.Now().UnixNano()
		//err = antidote.SendProtoNoCheck(replyType, reply, conn)
		/*if replyType == antidote.StaticReadObjsReply { //TODO: Tmp, remove
		var apbBoundObj *proto.ApbBoundObject
		var apbBoundKey []byte
		if protoType == antidote.StaticRead {
			apbBoundObj = protobuf.(*proto.ApbStaticRead).GetPartialreads()[0].GetObject()
		} else {
			apbBoundObj = protobuf.(*proto.ApbStaticReadObjects).GetObjects()[0]
		}
		apbBoundKey = apbBoundObj.GetKey()
		var readResultProto pb.Message
		//var readResultProto []byte
		//var readResultProto tools.Triple[[]crdt.State, antidote.TransactionId, clocksi.Timestamp]
		var protoPos byte
		//Q1 is safe, it's literally only Q1.
		//Q2 can be 20~24 (region) + typesSize (0~50). TopK
		//Q20 then has a 4 digits year.
		//Q21 will have nats (00-24). TopSum
		//Q22 has two: one of Q22+region, another of q22AVG + region.
		//Others is straightforward.
		firstDigit := apbBoundKey[1] //[0] is 'q'
		if (firstDigit >= '3' && firstDigit <= '9') || (firstDigit == '1' && len(apbBoundKey) == 2) {
			protoPos = firstDigit - '1' //we take away '1', as queries start on '1', but the slice starts at 0.
		} else if firstDigit == '1' { //Q10-Q19, just check 2nd digit
			protoPos = 9 + (apbBoundKey[2] - '0') //Q10 will be on 9 (as Q1 is on 0)
		} else { //firstDigit is '2'. Cases: Q2, Q20, Q21, Q22.
			//Q2 is 4 to 5 length.
			//Q20 is 7 length.
			//Q21 is 5 length always.
			//Q22 is always 4 length. Q22AVG is always 7 length.
			if len(apbBoundKey) == 7 { //Q22 AVG or Q20. Check 'A'
				if apbBoundKey[3] == 'A' { //Q22 AVG
					protoPos = 22
				} else { //Q20
					protoPos = 19
				}
			} else { //Q2, Q21 or Q22. The length isn't enough to conclude. Numbers also aren't: they can overlap. Have to look at the CRDTType.
				//Example overlap: Q21 with nat 10: Q2110. Q2 with region 1, typeSize 10: Q2110.
				crdtType := apbBoundObj.GetType()
				if crdtType == proto.CRDTType_TOPK { //Q2
					protoPos = 1
				} else if crdtType == proto.CRDTType_TOPSUM { //Q21
					protoPos = 20
				} else { //Q22.
					protoPos = 21
				}
			}
		}
		readResultProto = queryResult[protoPos]
		wasNil = (readResultProto == nil)
		/*if readResultProto.First == nil {
			readResultProto.First, readResultProto.Second, readResultProto.Third = stateReply.First, stateReply.Second, stateReply.Third
			queryResult[protoPos] = readResultProto
			nQueryResult++
		}
		reply = antidote.CreateStaticReadResp(readResultProto.First, readResultProto.Second, readResultProto.Third, stateBuf)*/ /*
			if readResultProto == nil {
				readResultProto = antidote.CreateStaticReadResp(stateReply.First, stateReply.Second, stateReply.Third, stateBuf)
				queryResult[protoPos] = readResultProto
				nQueryResult++
			}
			reply = readResultProto*/
		/*if readResultProto == nil {
			apb := antidote.CreateStaticReadResp(stateReply.First, stateReply.Second, stateReply.Third, stateBuf)
			reply = apb
			size := apb.SizeVT() + 5
			buf := make([]byte, size)
			apb.MarshalToSizedBufferVT(buf[5:])
			binary.BigEndian.PutUint32(buf[0:4], uint32(size-4)) //include protoType.
			buf[4] = replyType
			readResultProto = buf
			queryResult[protoPos] = readResultProto
			nQueryResult++
		}
		//reply = readResultProto
		_, err = conn.Write(readResultProto)
		if err != nil {
			conn.Close()
			fmt.Printf("[PS]Error on sending proto to client: %s. Closing connection.\n", err)
			return
		}*/
		/*if !wasNil {
				collectBufsFromState(stateReply.First, stateBuf) //This is only needed when we don't convert to protobufs, as it's when we do this conversion that we mark the buffers for re-use.
			}
			//stateBuf.ReturnBufs() //Uncomment this for when storing directly the byte buf.
			//continue //Uncomment this for when storing directly the byte buf.
		}*/
		//ignore(wasNil)
		/*staticRead, ok := reply.(*proto.ApbStaticReadObjectsResp)
		if ok {
			objs := staticRead.Objects
			if len(objs.Objects) == 2 {
				if objs.Objects[0].GetPartread().GetMap() == nil {
					fmt.Printf("[PS]Q22 protobuf detected, but it's first phase: ignoring.")
				} else {
					fmt.Printf("[PS]Q22 protobuf reply detected.\n")
					fmt.Printf("[PS]First object: %+v.\n", objs.Objects[0])
					fmt.Printf("[PS]Second object: %+v.\n", objs.Objects[1])
					//buf := make([]byte, staticRead.SizeVT())
					outBuf = outBuf[:staticRead.SizeVT()]
					for i := 0; i < len(outBuf); i++ {
						outBuf[i] = 0
					}
					nWritten, err := staticRead.MarshalToSizedBufferVT(outBuf)
					if err != nil {
						fmt.Printf("[PS]Error on marshalling Q22 reply: %s\n", err)
					}
					if nWritten != len(outBuf) {
						fmt.Printf("[PS]Error on marshalling Q22 reply: marshalled size %d does not match expected size %d\n", nWritten, len(outBuf))
					}
					newProto := (&proto.ApbStaticReadObjectsResp{})
					err = newProto.UnmarshalVTUnsafe(outBuf)
					if err != nil {
						fmt.Printf("[PS]Error on unmarshalling Q22 reply: %s\n", err)
					}
					fmt.Printf("[PS](VT)Marshalled and unmarshalled Q22 reply.\n")
					firstP, secondP := newProto.Objects.Objects[0].GetPartread().GetMap().Getvalues.GetValues(), newProto.Objects.Objects[1].GetPartread().GetMap().Getvalues.GetValues()
					fmt.Printf("[PS]First object: (len %d) %+v.\n", len(firstP), newProto.Objects.Objects[0])
					fmt.Printf("[PS]Second object: (len %d) %+v.\n", len(secondP), newProto.Objects.Objects[1])
					normalUnmP := (&proto.ApbStaticReadObjectsResp{})
					err = pb.Unmarshal(outBuf, normalUnmP)
					if err != nil {
						fmt.Printf("[PS]Error on normal unmarshalling Q22 reply: %s\n", err)
					}
					fmt.Printf("[PS](Google PB)Normally unmarshalled Q22 reply.\n")
					firstP, secondP = normalUnmP.Objects.Objects[0].GetPartread().GetMap().Getvalues.GetValues(), normalUnmP.Objects.Objects[1].GetPartread().GetMap().Getvalues.GetValues()
					fmt.Printf("[PS]First object: (len %d) %+v.\n", len(firstP), normalUnmP.Objects.Objects[0])
					fmt.Printf("[PS]Second object: (len %d) %+v.\n", len(secondP), normalUnmP.Objects.Objects[1])
					outBuf = outBuf[:cap(outBuf)]
				}
			}
		}*/
		//antidote.SendProto(replyType, reply, conn)
		err, outBuf = antidote.SendProtoReusableBufVT(replyType, reply, conn, outBuf)
		/*if !wasNil { //This if is only needed when we cache the protobufs directly.
			stateBuf.ReturnBufs()
		} else { //Only needed when we cache the protobufs directly.
			stateBuf.Reset()
		}*/
		stateBuf.ReturnBufs()
		if err != nil {
			conn.Close()
			fmt.Printf("[PS]Error on sending proto to client: %s. Closing connection.\n", err)
			return
		}
		end := time.Now().UnixNano()
		diff := end - start
		if diff >= int64(1*time.Second) {
			fmt.Printf("[PS%d]Client request took too long - %.2fms!!! Request type: %v\n", clientId&0xFFFF, float64(diff)/float64(time.Millisecond), protoType)
		}
		//buf = make([]byte, 1000) //TODO: Remove.
		//tsEnd := time.Now().UnixNano()
		//fmt.Printf("[PS]Protobuf sending took %d microseconds.\n", (tsEnd-tsStart)/int64(time.Duration(time.Microsecond)))
	}
S2SFinallizer: //We jump here if this was a S2S connection and the connection was terminated.
	if isS2SConn {
		//Can do something here if needed, e.g., print something. For now, we do nothing.
	} else {
		//Client-connection. But we never break out of the loop if it's a client connection, so the code never reaches here.
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
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, stateBuf *crdt.BufsToReturnToPool, clientBufs *antidote.ClientBuffers) (respProto *proto.ApbStaticReadObjectsResp) {

	//replyChan, txnId := sendTMStaticReadObjectsRequest(proto, tmChan, clientId, clientBufs)
	txnId := sendTMStaticReadObjectsRequest(proto, tmChan, clientId, clientBufs)
	clientBufs.ReuseReadProtos()
	reply := <-clientBufs.TMStaticReadChan
	//close(replyChan)
	return antidote.CreateStaticReadRespReuse(reply.States, txnId, reply.Timestamp, stateBuf, clientBufs.StaticReadRespProto)
	//return antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp, stateBuf)
}

func sendTMStaticReadObjectsRequest(proto *proto.ApbStaticReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, bufs *antidote.ClientBuffers) (txnId antidote.TransactionId) { //(replyChan chan antidote.TMStaticReadReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptorReuse(proto.GetTransaction().GetTimestamp(), bufs.Clk)
	objs := antidote.ProtoObjectsToAntidoteObjectsReuse(proto.GetObjects(), bufs.ReadBuf)
	//replyChan = make(chan antidote.TMStaticReadReply)
	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: bufs.TMStaticReadChan}, txnId, clientClock)
	bufs.ReadBuf = objs
	return txnId
	//return replyChan, txnId
}

func handleStaticRead(proto *proto.ApbStaticRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, stateBuf *crdt.BufsToReturnToPool, clientBufs *antidote.ClientBuffers) (respProto *proto.ApbStaticReadObjectsResp) {

	txnId := sendTMStaticReadRequest(proto, tmChan, clientId, clientBufs)
	clientBufs.ReuseReadProtos()
	reply := <-clientBufs.TMStaticReadChan
	//close(replyChan)
	return antidote.CreateStaticReadRespReuse(reply.States, txnId, reply.Timestamp, stateBuf, clientBufs.StaticReadRespProto)
	//return antidote.CreateStaticReadResp(reply.States, txnId, reply.Timestamp, stateBuf)
}

func sendTMStaticReadRequest(proto *proto.ApbStaticRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, bufs *antidote.ClientBuffers) (txnId antidote.TransactionId) { //(replyChan chan antidote.TMStaticReadReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptorReuse(proto.GetTransaction().GetTimestamp(), bufs.Clk)
	objs := antidote.ProtoReadToAntidoteObjectsReuse(proto.GetFullreads(), proto.GetPartialreads(), bufs.ReadBuf)
	//replyChan = make(chan antidote.TMStaticReadReply)
	tmChan <- createTMRequest(antidote.TMStaticReadArgs{ReadParams: objs, ReplyChan: bufs.TMStaticReadChan}, txnId, clientClock)
	bufs.ReadBuf = objs
	return txnId
	//return replyChan, txnId
}

// Return the internal state, to allow reusage of protobufs, for performance debugging.
func handleStaticReadDebug(proto *proto.ApbStaticRead,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, bufs *antidote.ClientBuffers) (resp tools.Triple[[]crdt.State, antidote.TransactionId, clocksi.Timestamp]) {

	txnId := sendTMStaticReadRequest(proto, tmChan, clientId, bufs)
	reply := <-bufs.TMStaticReadChan
	return tools.Triple[[]crdt.State, antidote.TransactionId, clocksi.Timestamp]{First: reply.States, Second: txnId, Third: reply.Timestamp}
}

// Return the internal state, to allow reusage of protobufs, for performance debugging.
func handleStaticReadObjectsDebug(proto *proto.ApbStaticReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, bufs *antidote.ClientBuffers) (resp tools.Triple[[]crdt.State, antidote.TransactionId, clocksi.Timestamp]) {

	txnId := sendTMStaticReadObjectsRequest(proto, tmChan, clientId, bufs)
	reply := <-bufs.TMStaticReadChan
	return tools.Triple[[]crdt.State, antidote.TransactionId, clocksi.Timestamp]{First: reply.States, Second: txnId, Third: reply.Timestamp}
}

func handleStaticUpdateObjects(proto *proto.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, bufs *antidote.ClientBuffers) (respProto *proto.ApbCommitResp) {

	//replyChan, _ := sendTMStaticUpdateObjects(proto, tmChan, clientId, bufs)
	sendTMStaticUpdateObjects(proto, tmChan, clientId, bufs)
	bufs.ReuseUpdateProtos()
	reply := <-bufs.TMStaticUpdateChan
	//close(replyChan)
	//ignore(reply.Err)
	return antidote.CreateCommitOkRespReuse(reply.TransactionId, reply.Timestamp, bufs.CommitRespProto)
}

/*func handleStaticUpdateObjects(proto *proto.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (respProto *proto.ApbCommitResp) {

	replyChan, _ := sendTMStaticUpdateObjects(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	ignore(reply.Err)
	return antidote.CreateCommitOkResp(reply.TransactionId, reply.Timestamp)
}*/

func sendTMStaticUpdateObjects(proto *proto.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, bufs *antidote.ClientBuffers) { //(replyChan chan antidote.TMStaticUpdateReply, txnId antidote.TransactionId) {

	txnId, clk := antidote.DecodeTxnDescriptorReuse(proto.GetTransaction().GetTimestamp(), bufs.Clk)
	upds := antidote.ProtoUpdateOpToAndidoteUpdateReuse(proto.GetUpdates(), bufs.Upds)
	tmChan <- createTMRequest(antidote.TMStaticUpdateArgs{UpdateParams: upds, ReplyChan: bufs.TMStaticUpdateChan}, txnId, clk)
	bufs.Clk, bufs.Upds = clk, upds
}

/*func sendTMStaticUpdateObjects(proto *proto.ApbStaticUpdateObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId) (replyChan chan antidote.TMStaticUpdateReply, txnId antidote.TransactionId) {

	txnId, clientClock := antidote.DecodeTxnDescriptor(proto.GetTransaction().GetTimestamp())
	updates := antidote.ProtoUpdateOpToAntidoteUpdate(proto.GetUpdates())
	replyChan = make(chan antidote.TMStaticUpdateReply)
	tmChan <- createTMRequest(antidote.TMStaticUpdateArgs{UpdateParams: updates, ReplyChan: replyChan}, txnId, clientClock)
	return replyChan, txnId
}*/

func handleReadObjects(proto *proto.ApbReadObjects,
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, stateBuf *crdt.BufsToReturnToPool) (respProto *proto.ApbReadObjectsResp) {

	replyChan, _ := sendTMReadObjects(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	return antidote.CreateReadObjectsResp(reply, stateBuf)
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
	tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, stateBuf *crdt.BufsToReturnToPool) (respProto *proto.ApbReadObjectsResp) {

	replyChan, _ := sendTMRead(proto, tmChan, clientId)
	reply := <-replyChan
	close(replyChan)
	return antidote.CreateReadObjectsResp(reply, stateBuf)
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

// Used when it is known from the start that the connection is S2S.
func handleS2SConn(conn net.Conn, reqChan chan antidote.TransactionManagerRequest, replyChan chan antidote.TMS2SReply, clientId antidote.ClientId, protoType byte, msg pb.Message, inBuf, outBuf []byte, stateBuf *crdt.BufsToReturnToPool, clientBufs *antidote.ClientBuffers) {
	switch protoType {
	case antidote.ServerConnReplicaID:
		protobuf := msg.(*proto.ApbServerConnReplicaID)
		reqChan <- createTMRequest(antidote.TMReplicaID{ReplicaID: uint16(protobuf.GetReplicaID()), IP: protobuf.GetMyIP(), Buckets: protobuf.GetMyBuckets(), ReplyChan: replyChan}, 0, nil)
	case antidote.ServerConn:
		reqChan <- createTMRequest(antidote.TMServerConn{ReplyChan: replyChan}, 0, nil)
	}

	go s2sReplySender(conn, replyChan, stateBuf, clientBufs, outBuf)
	handleS2SRequests(conn, reqChan, inBuf, clientBufs)
}

// Refactored S2S handler.
// The idea now is that, when processConn receives a new S2S connection (i.e., ServerConnReplicaID or ServerConn), this handler is called to manage this conn as a S2S-only connection.
func upgradeToS2SConn(conn net.Conn, tm *antidote.TransactionManager, tmChan chan antidote.TransactionManagerRequest, clientId antidote.ClientId, protoType byte, msg pb.Message, inBuf, outBuf []byte, stateBuf *crdt.BufsToReturnToPool, clientBufs *antidote.ClientBuffers) {
	replyChan := make(chan antidote.TMS2SReply, 10)
	reqChan := tm.MakeS2SReqChan()
	switch protoType {
	case antidote.ServerConnReplicaID:
		protobuf := msg.(*proto.ApbServerConnReplicaID)
		tmChan <- createTMRequest(antidote.TMReplicaID{ReplicaID: uint16(protobuf.GetReplicaID()), IP: protobuf.GetMyIP(), Buckets: protobuf.GetMyBuckets(), ReplyChan: replyChan, ReqChan: reqChan}, 0, nil)
	case antidote.ServerConn:
		tmChan <- createTMRequest(antidote.TMServerConn{ReplyChan: replyChan, ReqChan: reqChan}, 0, nil)
	}
	*clientBufs.PbBuffers = proto.PbBuffers{}
	clientBufs.S2SInit()

	go s2sReplySender(conn, replyChan, stateBuf, clientBufs, outBuf)
	handleS2SRequests(conn, reqChan, inBuf, clientBufs)
}

func s2sReplySender(conn net.Conn, replyChan chan antidote.TMS2SReply, stateBuf *crdt.BufsToReturnToPool, clientBufs *antidote.ClientBuffers, outBuf []byte) {
	//Note that stateBuf is only used by this goroutine, but clientBufs is shared with S2S receiver. However they use separate fields from clientBufs.
	//var msg pb.Message
	var err error
	/*idsReply := tools.NewSliceWithCounter[uint64](1000)
	var startID, endID uint64
	go func() {
		lastPrinted := 0
		for {
			time.Sleep(2 * time.Second)
			if !idsReply.IsEmpty() && lastPrinted < idsReply.Len() {
				ids := idsReply.ToSlice()
				fmt.Printf("[PS]Sent S2S replies with client IDs: %v\n", ids[lastPrinted:])
				lastPrinted = len(ids)
			}
		}
	}()*/
	replyWrapper := &proto.S2SWrapperReply{ClientID: new(uint64), MsgID: new(proto.WrapperType)}

	for {
		//Wait on replyChan forever, do a switch with received item, send back on connection
		for wrapper := range replyChan {
			//startID = wrapper.ClientID
			//Create protobuf
			switch reply := wrapper.Reply.(type) {
			case antidote.TMStaticReadReply:
				//msg = antidote.CreateStaticReadRespReuse(reply.States, wrapper.TxnID, reply.Timestamp, stateBuf, clientBufs.StaticReadRespProto)
				replyWrapper.StaticReadObjs = antidote.CreateStaticReadRespReuse(reply.States, wrapper.TxnID, reply.Timestamp, stateBuf, clientBufs.StaticReadRespProto)
			case antidote.TMStartTxnReply:
				//msg = antidote.CreateStartTransactionResp(wrapper.TxnID, reply.Timestamp)
				replyWrapper.StartTxn = antidote.CreateStartTransactionResp(wrapper.TxnID, reply.Timestamp)
			case []crdt.State:
				//msg = antidote.CreateReadObjectsResp(reply, stateBuf)
				replyWrapper.ReadObjs = antidote.CreateReadObjectsResp(reply, stateBuf)
			case antidote.TMUpdateReply:
				//msg = antidote.CreateOperationResp()
				replyWrapper.Upd = antidote.CreateOperationResp()
			case antidote.TMStaticUpdateReply:
				//msg = antidote.CreateCommitOkRespReuse(wrapper.TxnID, reply.Timestamp, clientBufs.CommitRespProto)
				replyWrapper.CommitTxn = antidote.CreateCommitOkRespReuse(wrapper.TxnID, reply.Timestamp, clientBufs.CommitRespProto)
			case antidote.TMCommitReply:
				//msg = antidote.CreateCommitOkRespReuse(wrapper.TxnID, reply.Timestamp, clientBufs.CommitRespProto)
				replyWrapper.CommitTxn = antidote.CreateCommitOkRespReuse(wrapper.TxnID, reply.Timestamp, clientBufs.CommitRespProto)
			default:
				fmt.Printf("[PS]S2S unknown proto type (%d, %T, %v+)\n", wrapper.ReplyType, reply, reply)
				panic(fmt.Sprintf("[PS]S2S unknown proto type (%d, %T, %v+)\n", wrapper.ReplyType, reply, reply))
			}
			*replyWrapper.ClientID, *replyWrapper.MsgID = wrapper.ClientID, wrapper.ReplyType
			err, outBuf = antidote.SendProtoReusableBufVT(antidote.S2SReply, replyWrapper, conn, outBuf)
			//err, outBuf = antidote.SendProtoReusableBufVT(antidote.S2SReply, antidote.CreateS2SWrapperReplyProto(wrapper.ClientID, wrapper.ReplyType, msg), conn, outBuf)
			//protoToSend := antidote.CreateS2SWrapperReplyProto(wrapper.ClientID, wrapper.ReplyType, msg)
			//antidote.SendProtoS2SReplyDebug(antidote.S2SReply, protoToSend, conn)
			stateBuf.ReturnBufs()
			/*endID = protoToSend.GetClientID()
			if startID != endID {
				panic(fmt.Sprintf("[PS]S2S reply routine, clientID changed from start (%d) to end (%d).\n", startID, endID))
			}
			idsReply.Append(wrapper.ClientID)*/

			//Need to clean the re-used S2SWrapperReplyProto
			switch wrapper.Reply.(type) {
			case antidote.TMStaticReadReply:
				replyWrapper.StaticReadObjs = nil
			case antidote.TMStartTxnReply:
				replyWrapper.StartTxn = nil
			case []crdt.State:
				replyWrapper.ReadObjs = nil
			case antidote.TMStaticUpdateReply:
				replyWrapper.CommitTxn = nil
			case antidote.TMCommitReply:
				replyWrapper.CommitTxn = nil
			}

			if err != nil {
				conn.Close()
				fmt.Printf("[PS]Error on sending S2S reply proto to server: %s. Closing connection.\n", err)
			}
		}
	}
}

func handleS2SRequests(conn net.Conn, tmChan chan antidote.TransactionManagerRequest, inBuf []byte, clientBufs *antidote.ClientBuffers) {
	//Note: It is not safe here to re-use non-protobuf buffers ([]crdt.UpdateObjectParams or []crdt.ReadObjectParams), as this connection may handle multiple clients while TM is processing.
	//We'd need some kind of pooling mechanism to re-use those buffers, or some identification and having s2sReplySender notify us when a certain buffer is safe to be re-used.
	//Protobuf buffers can still be re-used here through.
	var protoType byte
	var protobuf pb.Message
	var err error
	var req antidote.TMRequestArgs
	var txnId antidote.TransactionId
	var clientClock clocksi.Timestamp
	var clientID /*, endID*/ uint64

	/*idsRec := tools.NewSliceWithCounter[uint64](1000)
	go func() {
		lastPrinted := 0
		time.Sleep(1 * time.Second) //Give some difference to the S2SReplySender.
		for {
			time.Sleep(2 * time.Second)
			if !idsRec.IsEmpty() && lastPrinted < idsRec.Len() {
				ids := idsRec.ToSlice()
				fmt.Printf("[PS]Received S2S requests with client IDs: %v\n", ids[lastPrinted:])
				lastPrinted = len(ids)
			}
		}
	}()
	ignore(inBuf)*/
	//TODO: In theory we could use a pool (or slice) of byte slices and use ReceiveProtoReusableBufferVT with unsafe marshal for max memory re-usage.
	//However, this would imply having to ask for the s2sReplySender to give us back the byte slices when done (which he doesn't even have access to, only the replies? Maybe have to associate to clientID)
	//A less (memory-wise) efficient solution is to simply use the buffer only for reading in the bytes, and use unsafe when unmarshalling.
	//This way the byte slice can be re-used immediately. The protobuf info can also be re-used if we want to.
	//In unmarshallProtoVT, used by ReceiveProtoReusableBufferVT, we force S2S messages to use the safe version.
	//TODO: Support re-usafe of S2S protobufs? At least the wrapper part. It is safe as we won't use the protobuf after sending the request to TM.
	//(Just be careful with string/byte slice re-using)
	for {
		protoType, protobuf, err, inBuf = antidote.ReceiveProtoReusableBufferVTClientBuf(conn, inBuf, clientBufs)
		//protoType, protobuf, err = antidote.ReceiveProtoVT(conn)
		if err != nil {
			if err == io.EOF {
				conn.Close()
				tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
			} else {
				conn.Close()
				date := time.Now().String()
				fmt.Printf("[ProtoServer]Error on reading proto from client, closing connection.. Type: %v, proto: %v, error: %s, time: %s\n", protoType, protobuf, err, date)
				tmChan <- antidote.TransactionManagerRequest{Args: antidote.TMConnLostArgs{}}
			}
			break
		} else {
			s2sPb, ok := protobuf.(*proto.S2SWrapper)
			if !ok {
				fmt.Printf("[ProtoServer]Error: expected S2SWrapper, got %T\n", protobuf)
				panic(fmt.Sprintf("[ProtoServer]Error: expected S2SWrapper, got %T\n", protobuf))
			}
			clientID = s2sPb.GetClientID()
			switch s2sPb.GetMsgID() {
			case proto.WrapperType_STATIC_READ_OBJS:
				inProto := s2sPb.StaticReadObjs
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
				objs := antidote.ProtoObjectsToAntidoteObjects(inProto.GetObjects())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticReadArgs{ReadParams: objs}}
				tmChan <- createTMRequest(req, txnId, clientClock)
			case proto.WrapperType_STATIC_READ:
				inProto := s2sPb.StaticRead
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
				objs := antidote.ProtoReadToAntidoteObjects(inProto.GetFullreads(), inProto.GetPartialreads())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticReadArgs{ReadParams: objs}}
				tmChan <- createTMRequest(req, txnId, clientClock)
			case proto.WrapperType_STATIC_SINGLE_READ:
				inProto := s2sPb.SingleRead
				obj := antidote.S2SSingleReadToAntidote(inProto)
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMSingleReadArgs{ReadParams: obj}}
				tmChan <- createTMRequest(req, 578902378, nil) //Random txnID value
			case proto.WrapperType_STATIC_UPDATE:
				inProto := s2sPb.StaticUpd
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransaction().GetTimestamp())
				updates := antidote.ProtoUpdateOpToAntidoteUpdate(inProto.GetUpdates())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStaticUpdateArgs{UpdateParams: updates}}
				tmChan <- createTMRequest(req, txnId, clientClock)
			case proto.WrapperType_START_TXN:
				inProto := s2sPb.StartTxn
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTimestamp())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMStartTxnArgs{}}
				tmChan <- createTMRequest(req, txnId, clientClock)
			case proto.WrapperType_READ_OBJS:
				inProto := s2sPb.ReadObjs
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
				objs := antidote.ProtoObjectsToAntidoteObjects(inProto.GetBoundobjects())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMReadArgs{ReadParams: objs}}
				tmChan <- createTMRequest(req, txnId, clientClock)
			case proto.WrapperType_READ:
				inProto := s2sPb.Read
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
				objs := antidote.ProtoReadToAntidoteObjects(inProto.GetFullreads(), inProto.GetPartialreads())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMReadArgs{ReadParams: objs}}
				tmChan <- createTMRequest(req, txnId, clientClock)

			case proto.WrapperType_UPD:
				inProto := s2sPb.Upd
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
				updates := antidote.ProtoUpdateOpToAntidoteUpdate(inProto.GetUpdates())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMUpdateArgs{UpdateParams: updates}}
				tmChan <- createTMRequest(req, txnId, clientClock)

			case proto.WrapperType_COMMIT:
				inProto := s2sPb.CommitTxn
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMCommitArgs{}}
				tmChan <- createTMRequest(req, txnId, clientClock)

			case proto.WrapperType_ABORT:
				inProto := s2sPb.AbortTxn
				txnId, clientClock = antidote.DecodeTxnDescriptor(inProto.GetTransactionDescriptor())
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.TMAbortArgs{}}
				tmChan <- createTMRequest(req, txnId, clientClock)

			case proto.WrapperType_BC_PERMS_REQ:
				inProto := s2sPb.BcPermsReq
				req = antidote.TMS2SRequest{ClientID: clientID, Args: antidote.ProtoBCPermissionsReqToTM(inProto)}
				tmChan <- createTMRequest(req, 578902378, nil) //Random txID value

			default:
				fmt.Printf("[PS]S2S unknown wrapper type: %d\n", s2sPb.GetMsgID())
				panic(fmt.Sprintf("[PS]S2S unknown wrapper type: %d\n", s2sPb.GetMsgID()))
			}
			/*if s2sPb.GetMsgID() != proto.WrapperType_STATIC_UPDATE { //TODO: Remove, just for debug.
				fmt.Printf("[PS]Unexpected S2S wrapper type: was expecting STATIC_UPDATE, got %d\n", *s2sPb.MsgID)
				panic(fmt.Sprintf("[PS]Unexpected S2S wrapper type: was expecting STATIC_UPDATE, got %d\n", *s2sPb.MsgID))
			}
			endID = s2sPb.GetClientID()
			if clientID != endID {
				panic(fmt.Sprintf("[PS]S2S request routine, clientID changed from start (%d) to end (%d).\n", clientID, endID))
			}
			idsRec.Append(clientID)*/
		}
	}
}

/*
func handleServerConnReplicaID(protobuf *proto.ApbServerConnReplicaID, tmChan chan antidote.TransactionManagerRequest, conn net.Conn,
	stateBuf *crdt.BufsToReturnToPool, clientBufs *antidote.ClientBuffers) chan antidote.TMS2SReply {
	fmt.Printf("[PS]Got ServerConnReplicaID from %d at %s\n", protobuf.GetReplicaID(), time.Now().Format("15:04:05:000"))
	if clientBufs == nil {
		clientBufs = antidote.InitializeClientBuffers(200) //Arbitrary size that is definitely big enough.
	}
	tmChan <- createTMRequest(antidote.TMReplicaID{ReplicaID: uint16(protobuf.GetReplicaID()), IP: protobuf.GetMyIP(), Buckets: protobuf.GetMyBuckets()}, 0, nil)
	return handleServerConn(tmChan, conn, stateBuf, clientBufs)
}

func handleServerConn(tmChan chan antidote.TransactionManagerRequest, conn net.Conn,
	stateBuf *crdt.BufsToReturnToPool, clientBufs *antidote.ClientBuffers) chan antidote.TMS2SReply {
	//Note that stateBuf and clientBufs are shared with the sender routine.
	//
	replyChan := make(chan antidote.TMS2SReply, 10)
	tmChan <- createTMRequest(antidote.TMServerConn{ReplyChan: replyChan}, 0, nil)

	go func() {
		buf := make([]byte, START_CLIENT_BUF_SIZE)
		ignore(buf)
		var err error
		ignore(err)
		var msg pb.Message
		//fmt.Println("[PS]S2S receiver - ready")
		//Wait on replyChan forever, do a switch with received item, send back on connection
		for wrapper := range replyChan {
			debugChan := make(chan struct{}, 1)
			go func(dChan chan struct{}) {
				select {
				case <-dChan:
					return
				case <-time.After(10 * time.Second):
					fmt.Printf("[PS]S2S reply sender - stuck! Last msg: %T, %+v ClientID: %d. TxnID: %d. ReplyType: %d.\n",
						wrapper.Reply, wrapper.Reply, wrapper.ClientID, wrapper.TxnID, wrapper.ReplyType)
					time.Sleep(2000)
					os.Exit(0)
				}
			}(debugChan)
			//fmt.Println("[PS]S2S receiver - got reply (clientID, replyType)", wrapper.ClientID, wrapper.ReplyType)
			switch reply := wrapper.Reply.(type) {
			case antidote.TMStaticReadReply:
				msg = antidote.CreateStaticReadRespReuse(reply.States, wrapper.TxnID, reply.Timestamp, stateBuf, clientBufs.StaticReadRespProto)
			case antidote.TMStartTxnReply:
				msg = antidote.CreateStartTransactionResp(wrapper.TxnID, reply.Timestamp)
			case []crdt.State:
				msg = antidote.CreateReadObjectsResp(reply, stateBuf)
			case antidote.TMUpdateReply:
				msg = antidote.CreateOperationResp()
			case antidote.TMStaticUpdateReply:
				msg = antidote.CreateCommitOkRespReuse(wrapper.TxnID, reply.Timestamp, clientBufs.CommitRespProto)
			case antidote.TMCommitReply:
				msg = antidote.CreateCommitOkRespReuse(wrapper.TxnID, reply.Timestamp, clientBufs.CommitRespProto)
			default:
				fmt.Printf("[PS]S2S unknown proto type (%d, %T, %v+)\n", wrapper.ReplyType, reply, reply)
			}
			//fmt.Println("[PS]S2S sending reply")
			//err = antidote.SendProtoNoCheck(antidote.S2SReply, antidote.CreateS2SWrapperReplyProto(wrapper.ClientID, wrapper.ReplyType, msg), conn) //TODO: Go back.
			antidote.SendProtoS2SReplyDebug(antidote.S2SReply, antidote.CreateS2SWrapperReplyProto(wrapper.ClientID, wrapper.ReplyType, msg), conn) //TODO: Go back.
			//err, buf = antidote.SendProtoReusableBufVT(antidote.S2SReply, antidote.CreateS2SWrapperReplyProto(wrapper.ClientID, wrapper.ReplyType, msg), conn, buf) //TODO: UNDO
			//fmt.Println("[PS]S2S sent reply")
			stateBuf.ReturnBufs()
			debugChan <- struct{}{}
		}
	}()

	return replyChan
}

func handleServerToServer(protobf *proto.S2SWrapper, tmChan chan antidote.TransactionManagerRequest, s2sChan chan antidote.TMS2SReply,
	conn net.Conn, tm *antidote.TransactionManager, stateBufs *crdt.BufsToReturnToPool, clientBufs *antidote.ClientBuffers) {
	//"Just" send appropriate request
	var req antidote.TMRequestArgs
	var txnId antidote.TransactionId
	var clientClock clocksi.Timestamp
	var reply interface{}
	var replyType proto.WrapperType
	clientID := protobf.GetClientID()
	//fmt.Println("[PS]S2S request type:", *protobf.MsgID)
	debugChan := make(chan struct{}, 1)
	go func(dChan chan struct{}) {
		select {
		case <-dChan:
			return
		case <-time.After(10 * time.Second):
			fmt.Printf("[PS]S2S applier - stuck! Last msg type: %T. ClientID: %d. TxnID: %d. ReplyType: %d. Clk: %s.\n",
				*protobf.MsgID, clientID, txnId, replyType, clientClock.ToString())
			time.Sleep(1500)
			os.Exit(0)
		}
	}(debugChan)
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
		panic(0)
	}
	if *protobf.MsgID != proto.WrapperType_STATIC_UPDATE {
		fmt.Printf("[PS]Unexpected S2S wrapper type: was expecting STATIC_UPDATE, got %d\n", *protobf.MsgID)
		os.Exit(0)
	}
	//Before TxnID was hardcoded to be 2 on TMS2SReply. Why?
	s2sChan <- antidote.TMS2SReply{ClientID: clientID, TxnID: txnId, ReplyType: replyType, Reply: reply}
	debugChan <- struct{}{}
}*/

func handleMultiClient(conn net.Conn, tmChans []chan antidote.TransactionManagerRequest, replyChan chan antidote.TMMultiClientReply, nClients int, clientId antidote.ClientId, txnDescSize int) {
	go handleMultiClientReplies(conn, replyChan)
	clientIds := make([]antidote.ClientId, nClients)
	clientIds[0] = clientId
	clientBufs := make([]*antidote.ClientBuffers, nClients)
	rng := rand.New(rand.NewSource(int64(clientId)))
	for i := 1; i < nClients; i++ {
		clientIds[i] = antidote.ClientId(rng.Uint64())
		clientBufs[i] = antidote.InitializeClientBuffers(txnDescSize)
	}

	var protobuf pb.Message
	var protoType byte
	var client uint16
	var err error
	buf := make([]byte, START_CLIENT_BUF_SIZE) //Re-usable byte buffer for ReceiveProtoMultiClient.
	doesUpdates := false

	fmt.Printf("[ProtoServer]Handling multi-client connection with %d clients\n", nClients)
	for {
		//protoType, client, protobuf, err = antidote.ReceiveProtoMultiClient(conn)
		protoType, client, protobuf, err, buf = antidote.ReceiveProtoMultiClientReusableBuf(conn, buf)
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
			if !doesUpdates {
				doesUpdates = true
				for i := 0; i < nClients; i++ {
					clientBufs[i].UpdateInit()
				}
			}
			sendTMStartTxn(protobuf.(*proto.ApbStartTransaction), tmChans[client], clientIds[client])
		case antidote.AbortTrans:
			sendTMAbortTxn(protobuf.(*proto.ApbAbortTransaction), tmChans[client], clientIds[client])
		case antidote.CommitTrans:
			sendTMCommitTxn(protobuf.(*proto.ApbCommitTransaction), tmChans[client], clientIds[client])
		case antidote.StaticUpdateObjs:
			if !doesUpdates {
				doesUpdates = true
				for i := 0; i < nClients; i++ {
					clientBufs[i].UpdateInit()
				}
			}
			sendTMStaticUpdateObjects(protobuf.(*proto.ApbStaticUpdateObjects), tmChans[client], clientIds[client], clientBufs[client])
		case antidote.StaticReadObjs:
			sendTMStaticReadObjectsRequest(protobuf.(*proto.ApbStaticReadObjects), tmChans[client], clientIds[client], clientBufs[client])
		case antidote.StaticRead:
			sendTMStaticReadRequest(protobuf.(*proto.ApbStaticRead), tmChans[client], clientIds[client], clientBufs[client])
		}
	}
}

// Extra goroutine that will listen to replyChan and send the replies to the clients of this multi-client connection.
func handleMultiClientReplies(conn net.Conn, replyChan chan antidote.TMMultiClientReply) {
	var respProto pb.Message
	var replyType byte
	var err error
	buf := make([]byte, START_CLIENT_BUF_SIZE) //Re-usable byte buffer for SendProtoMultiClientNoCheck.
	stateBuf := crdt.NewBufsToReturn()         //Collects large buffers that are used by CRDT states. These buffers should be returned after the states are Marshalled (or after sent to the client).

	for {
		reply := <-replyChan
		//fmt.Printf("[ProtoServer][MultiClient]Got reply from TM: %v\n", reply)
		switch typedReply := reply.Reply.(type) {
		case antidote.TMStaticReadReply: //Replies to both static read and static read objects are the same
			respProto, replyType = antidote.CreateStaticReadResp(typedReply.States, reply.TxnId, typedReply.Timestamp, stateBuf), antidote.StaticReadObjsReply
		case antidote.TMStaticUpdateReply:
			respProto, replyType = antidote.CreateCommitOkResp(reply.TxnId, typedReply.Timestamp), antidote.CommitTransReply
		case []crdt.State: //Reply to both read and read objects are the same
			respProto, replyType = antidote.CreateReadObjectsResp(typedReply, stateBuf), antidote.ReadObjsReply
		case antidote.TMUpdateReply:
			respProto, replyType = antidote.CreateOperationResp(), antidote.OpReply
		case antidote.TMStartTxnReply:
			respProto, replyType = antidote.CreateStartTransactionResp(reply.TxnId, typedReply.Timestamp), antidote.StartTransReply
		case antidote.TMCommitReply:
			respProto, replyType = antidote.CreateCommitOkResp(reply.TxnId, typedReply.Timestamp), antidote.CommitTransReply

		}
		//err = antidote.SendProtoMultiClientNoCheck(replyType, uint16(reply.ClientID), respProto, conn)
		err, buf = antidote.SendProtoMultiClientNoCheckReusableBuf(replyType, uint16(reply.ClientID), respProto, conn, buf)
		if err != nil {
			conn.Close()
			fmt.Println("[ProtoServer]Error on sending proto to multi client:", err)
			return
		}
	}

}

func handleProtoTestRead(protobuf pb.Message, protoType byte, stateBuf *crdt.BufsToReturnToPool) *proto.ApbStaticReadObjectsResp {
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
	return antidote.CreateStaticReadResp(states, txnId, clientClock, stateBuf)
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
	return antidote.CreateStaticReadResp(states, txnId, clientClock, stateBuf)*/
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
	if shared.IsGCDisabled {
		fmt.Println("[PS][WARNING]PotionDB's GC (Garbage Collection) is disabled. While Go's GC will still work, memory usage may grow infinitely as updates get applied. " +
			"This should only be used for debugging/specific performance analysis.")
	}
}

func startProfiling(configs *tools.ConfigLoader) {
	var err error
	if profileCPUString, has := configs.GetAndHasConfig(CPU_PROFILE_KEY); has {
		profileCPU, err = strconv.ParseBool(profileCPUString)
		if err != nil {
			profileCPU = true
			delay, err := strconv.ParseInt(profileCPUString, 10, 64)
			if err != nil {
				fmt.Printf("[WARNING]Invalid CPU profilling settings, CPU profile is off.\n")
				profileCPU = false
			} else {
				go func(waitTime int64) {
					fmt.Printf("Waiting for %dms before starting CPU profiling...\n", waitTime)
					time.Sleep(time.Duration(waitTime) * time.Millisecond)
					file, err := os.Create(configs.GetConfig(CPU_FILE_KEY))
					utilities.CheckErr("Failed to create CPU profile file: ", err)
					fmt.Println("CPU profile file created at: ", file.Name())
					pprof.StartCPUProfile(file)
					fmt.Println("Started CPU profiling")
				}(delay)
			}
		} else {
			if profileCPU {
				file, err := os.Create(configs.GetConfig(CPU_FILE_KEY))
				utilities.CheckErr("Failed to create CPU profile file: ", err)
				fmt.Println("CPU profile file created at: ", file.Name())
				pprof.StartCPUProfile(file)
				fmt.Println("Started CPU profiling")
			}
		}
	}
	if profileMemString, has := configs.GetAndHasConfig(MEM_PROFILE_KEY); has {
		profileMem, err = strconv.ParseBool(profileMemString)
		if err != nil {
			profileMem = true
			delay, err := strconv.ParseInt(profileMemString, 10, 64)
			if err != nil {
				fmt.Printf("[WARNING]Invalid memory profilling settings, memory profile is off.\n")
				profileMem = false
			} else {
				fileLoc := configs.GetConfig(MEM_FILE_KEY)
				dotPos := strings.LastIndex(fileLoc, ".")
				fileLoc = fileLoc[:dotPos] + "_base" + fileLoc[dotPos:]
				fmt.Printf("Memory profile with delay is on. A memory profile to use for diff will be generated under name %s.\n", fileLoc)
				go func(waitTime int64, fileLocation string) {
					time.Sleep(time.Duration(waitTime) * time.Millisecond)
					file, err := os.Create(fileLocation)
					utilities.CheckErr("Failed to create base Memory profile file: ", err)
					fmt.Println("Created memory profile file at ", file.Name())
					pprof.WriteHeapProfile(file)
					file.Close()
				}(delay, fileLoc)
			}
		} else if profileMem {
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
				utilities.CheckErr("Failed to create Memory profile file: ", err)
				defer file.Close()
				fmt.Println("Created memory profile file at ", file.Name())
				pprof.WriteHeapProfile(file)
			}
			fmt.Println("Profiles saved, closing...")
			//os.Exit(0)
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
	disableVM := flag.String("disableVM", "none", "if VM (Version Management) should be disabled. Mostly useful for measuring memory overhead. False by default")
	disableGC := flag.String("disableGC", "none", "if GC (Garbage Collection) should be disabled. False by default")
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
	cpuProfile := flag.String("cpuProfiling", "none", "if true, a Go log profile will be created regarding CPU usage. Alternatively, pass a delay (in ms) to delay CPU profile starting.")
	memoryProfile := flag.String("memProfiling", "none", "if true, a Go log profile will be created regarding memory usage.")
	memDebug := flag.String("memDebug", "none", "if true, prints some debug info regarding memory usage. Alternatively, can also specify the interval (in ms) for printing this info.")
	dataloadType := flag.String("dataloadType", "none", "if doing tpch dataload, whenever to use compressed (default) or raw dataload. This is mostly for debugging/testing purposes.")
	nPartitions := flag.String("nPartitions", "none", "number of partitions for the Materializer. This is usually better set via a config file, as it must be equal for all replicas. Do not set via command line unless debugging/experimenting.")

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
	if isFlagValid(*disableVM, "none") {
		configs.ReplaceConfig("disableVM", *disableVM)
	}
	if isFlagValid(*disableGC, "none") {
		configs.ReplaceConfig("disableGC", *disableGC)
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
	if isFlagValid(*cpuProfile, "none") {
		configs.ReplaceConfig(CPU_PROFILE_KEY, *cpuProfile)
	}
	if isFlagValid(*memoryProfile, "none") {
		configs.ReplaceConfig(MEM_PROFILE_KEY, *memoryProfile)
	}
	if isFlagValid(*memDebug, "none") {
		if _, err := strconv.ParseInt(*memDebug, 10, 64); err == nil {
			configs.ReplaceConfig(MEM_DEBUG_PERIOD, *memDebug)
			configs.ReplaceConfig(MEM_DEBUG, "true")
			fmt.Printf("[PS][LoadConfigs]Set MEM_DEBUG and MEM_DEBUG_PERIOD to %v %v.\n", configs.GetConfig(MEM_DEBUG), configs.GetConfig(MEM_DEBUG_PERIOD))
		} else {
			configs.ReplaceConfig(MEM_DEBUG, *memDebug)
			fmt.Printf("[PS][LoadConfigs]Set only MEM_DEBUG: no int detected. MEM_DEBUG set to %v.\n", configs.GetConfig(MEM_DEBUG))
		}
	}
	if isFlagValid(*dataloadType, "none") {
		tpch.DataloadType = *dataloadType
	}
	if isFlagValid(*nPartitions, "none") {
		configs.ReplaceConfig("nPartitions", *nPartitions)
	}
	//configs.ReplaceConfig("nPartitions", "64")
	fmt.Printf("[PS]DoDataload: %s; SF: %s; DataLoc: %s; Region: %s; Partitions: %d..\n", *doDataload, *sf, *dataLoc, *region, configs.GetIntConfig("nPartitions", 0))
	//fmt.Printf("[PS]nPartitions from command line: %v; from config file: %d.\n", *nPartitions, configs.GetIntConfig("nPartitions", 0))
	//fmt.Println(*doDataload)
	//fmt.Println(*sf)
	//fmt.Println(*dataLoc)
	//fmt.Println(*region)
	shared.IsReplDisabled = configs.GetBoolConfig("disableReplicator", shared.IsReplDisabled)
	shared.IsLogDisabled = configs.GetBoolConfig("disableLog", shared.IsLogDisabled)
	shared.IsReadWaitingDisabled = configs.GetBoolConfig("disableReadWaiting", shared.IsReadWaitingDisabled)
	shared.IsVMDisabled = configs.GetBoolConfig("disableVM", shared.IsVMDisabled)
	shared.IsGCDisabled = configs.GetBoolConfig("disableGC", shared.IsGCDisabled)
	protobufTestMode = configs.GetIntConfig("protoTestMode", 0)

	fmt.Printf("[PS]Disables: Repl: %v, Log: %v, ReadW: %v, VM: %v, GC: %v\n", shared.IsReplDisabled, shared.IsLogDisabled,
		shared.IsReadWaitingDisabled, shared.IsVMDisabled, shared.IsGCDisabled)

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

func forceGC() {
	fmt.Printf("[PS][WARNING]Forced GC is on!!!\n")
	go func() {
		var start, end int64
		for {
			time.Sleep(30 * time.Second)
			start = time.Now().UnixNano()
			runtime.GC()
			end = time.Now().UnixNano()
			fmt.Printf("[PS]Forced GC; waiting another 30s. GC took %d ms\n", (end-start)/int64(time.Millisecond))
		}
	}()
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
	shouldDebug, err, sleepTime := false, error(nil), 10000
	if debugMem, has := configs.GetAndHasConfig(MEM_DEBUG); has {
		shouldDebug, err = strconv.ParseBool(debugMem)
		sleepTime = configs.GetIntConfig(MEM_DEBUG_PERIOD, sleepTime)

	}
	fmt.Printf("[PS][DebugMemory]Values: MEM_DEBUG: %v; MEM_DEBUG_PERIOD: %v.\n", shouldDebug, sleepTime)
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

		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

func printMemStats(memStats *runtime.MemStats, maxAlloc uint64) {
	runtime.ReadMemStats(memStats)
	const MB = 1048576
	/*fmt.Printf("Total mem stolen from OS: %d MB\n", memStats.Sys/MB)
	if maxAlloc != 0 {
		maxAlloc = tools.Max(maxAlloc, memStats.Alloc)
		fmt.Printf("Max alloced: %d MB\n", maxAlloc/MB)
	}
	fmt.Printf("Currently alloced: %d MB\n", memStats.Alloc/MB)
	fmt.Printf("Mem that could be returned to OS: %d MB\n", (memStats.HeapIdle-memStats.HeapReleased)/MB)
	fmt.Printf("Heap stats: heap use %dMB; heap alloc %dMB; heap alloced but not used: %dMB; alloc %dMB; heap idle %dMB\n",
		memStats.HeapInuse/MB, memStats.HeapAlloc/MB, (memStats.HeapInuse-memStats.HeapAlloc)/MB, memStats.Alloc/MB, memStats.HeapIdle/MB)
	fmt.Printf("Number of objs still malloced: %d\n", memStats.HeapObjects)
	fmt.Printf("Largest heap size: %d MB\n", memStats.HeapSys/MB)
	fmt.Printf("Stack size stolen from OS: %d MB\n", memStats.StackSys/MB)
	fmt.Printf("Stack size in use: %d MB\n", memStats.StackInuse/MB)
	fmt.Printf("Number of goroutines: %d\n", runtime.NumGoroutine())
	fmt.Printf("Number of GC cycles: %d\n", memStats.NumGC)
	fmt.Println()
	*/
	if maxAlloc != 0 {
		maxAlloc = tools.Max(maxAlloc, memStats.Alloc)
		fmt.Printf("Curr/max/requested mem: %d/%d/%d MB\n", memStats.Alloc/MB, maxAlloc/MB, memStats.Sys/MB)
	} else {
		fmt.Printf("Curr/requested mem: %d/%d MB\n", memStats.Alloc/MB, memStats.Sys/MB)
	}
	fmt.Printf("Mem that could be returned to OS: %d MB\n", (memStats.HeapIdle-memStats.HeapReleased)/MB)
	fmt.Printf("Heap use/alloc: %d/%dMB; heap alloced unused: %dMB; heap idle %dMB\n",
		memStats.HeapInuse/MB, memStats.HeapAlloc/MB, (memStats.HeapInuse-memStats.HeapAlloc)/MB, memStats.HeapIdle/MB)
	fmt.Printf("Number of objs still malloced: %d. \t Largest heap size: %d MB\n", memStats.HeapObjects, memStats.HeapSys/MB)
	fmt.Printf("Stack size stolen from OS: %d MB. \t Stack size in use: %d MB\n", memStats.StackSys/MB, memStats.StackInuse/MB)
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
		fmt.Println("[PS]Received SIGTERM before startup finished. Shutting down forcefully after a brief delay.")
		//Wait a bit for possible profiling or similars
		time.Sleep(200 * time.Millisecond)
		os.Exit(1)
	case <-readyChan: //Nothing, just finish goroutine.

	}
}

// Debug-related method. If we read states but don't convert to protobufs, then the automatic releasing of buffers to pools won't work.
// So we need to do it manually.
func collectBufsFromState(states []crdt.State, stateBufs *crdt.BufsToReturnToPool) {
	//Usually it'll be a single state, but this code is written to be able to handle multiple
	for _, state := range states {
		switch s := state.(type) {
		//Emb maps may have other relevant states within.
		case crdt.EmbMapEntryState:
			collectBufsFromMapState(s.States, stateBufs)
		case crdt.EmbMapGetValueState:
			collectBufsFromState([]crdt.State{s.State}, stateBufs) //May be another EmbMap inside...
		case crdt.EmbMapGetValuesState:
			collectBufsFromMapState(s.States, stateBufs)
		case crdt.CounterArrayState:
			if len(s) >= shared.MIN_SLICE_POOL_SIZE {
				stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: []int64(s)})
			}
		}
		if state.GetCRDTType() == proto.CRDTType_MAP_COUNTER {
			switch s := state.(type) {
			case crdt.CounterMapState[int64]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_INT64, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[int32]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_INT32, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[int16]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_INT16, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[int8]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_INT8, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[float64]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_FLOAT64, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[float32]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_FLOAT32, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}

			}
		}
	}
}

// Copy from collectBufsFromState, but states is in a map (as in RWEmbMap states) instead of in a slice.
func collectBufsFromMapState(states map[string]crdt.State, stateBufs *crdt.BufsToReturnToPool) {
	for _, state := range states {
		switch s := state.(type) {
		//Emb maps may have other relevant states within.
		case crdt.EmbMapEntryState:
			collectBufsFromMapState(s.States, stateBufs)
		case crdt.EmbMapGetValueState:
			collectBufsFromState([]crdt.State{s.State}, stateBufs) //May be another EmbMap inside...
		case crdt.EmbMapGetValuesState:
			collectBufsFromMapState(s.States, stateBufs)
		case crdt.CounterArrayState:
			if len(s) >= shared.MIN_SLICE_POOL_SIZE {
				stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: []int64(s)})
			}
		}
		if state.GetCRDTType() == proto.CRDTType_MAP_COUNTER {
			switch s := state.(type) {
			case crdt.CounterMapState[int64]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_INT64, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[int32]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_INT32, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[int16]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_INT16, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[int8]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_INT8, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[float64]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_FLOAT64, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			case crdt.CounterMapState[float32]:
				if len(s.Pairs) >= shared.MIN_SLICE_POOL_SIZE {
					buf := crdt.CounterMapStateBufs{DataType: proto.DATAType_FLOAT32, PairsBuf: s.Pairs}
					if len(s.Data) > 0 {
						buf.DataBuf = s.Data
					}
					stateBufs.Bufs.Append(crdt.BufToReturn{CRDTType: s.GetCRDTType(), Buf: buf})
				}
			}
		}
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
