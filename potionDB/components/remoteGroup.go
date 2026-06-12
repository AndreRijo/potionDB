package components

import (
	fmt "fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/potionDB/utilities"

	//pb "github.com/golang/protobuf/proto"
	"github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

//Handles multiple remoteConnections.go. Abstracts multiple RabbitMQ instances as if it was one only.
//Later on this will probably also be responsible for finding other replicas/datacenters.

type RemoteGroup struct {
	ourConn   *RemoteConn        //connection to this replica's/datacenter's RabbitMQ instance
	conns     []*RemoteConn      //groups to listen msgs from (includes ourConn)
	groupChan chan ReplicatorMsg //Groups requests sent by each remoteConnection.
	replicaID uint16             //For msgs purposes
	nReplicas uint16             //This counter is incremented as soon as a connection is attempted to be established. Helps with uniquely identifying the connections.
	knownIPs  map[string]uint16  //Used to verify if a received join corresponds to an already known replica or not. Stores the position in conns of each replica.
	workChan  chan RCWork
}

type GroupOrErr struct {
	*RemoteConn
	error
	index int
}

type RCWork interface {
	DoWork(replicaID uint16, buffers RCProtoBuffers)
}

// Holds re-usable protobuf buffers. Not all implementations of RCWork must support re-usable buffers, but they must include this struct in the DoWork method regardless.
type RCProtoBuffers struct {
	RemoteBktTxnProtoBuf *proto.ProtoReplicateTxn
}

/*type MarshallWork struct {
	BucketOps map[string]map[int][]crdt.UpdateObjectParams
	Txn       RemoteTxn
	ReplyChan chan MarshallWorkReply
}

type MarshallWorkReply struct {
	Result    []PairKeyBytes
	TxnSendId int32
}

type GroupMarshallWork struct {
	Bucket    string
	Txns      []RemoteTxn
	BktTxns   []RemoteTxn
	ReplyChan chan PairKeyBytes //Actually stores bucket, data
}*/

type ReplMarshallWork struct {
	Bucket    string
	Txn       RemoteTxn
	ReplyChan chan PairKeyBytes //Pair of Key (bucket), data (proto bytes)
	DebugChan chan any          //TODO: Comment.
}

type ReplMarshalBytePool struct {
	pools [5]sync.Pool
}

const (
	defaultListenerSize    = 200
	MIN_REPL_POOL_BUF_SIZE = 1000
)

var othersIPList []string

var replMarshalByteBufs = initReplMarshalPool()

func initReplMarshalPool() ReplMarshalBytePool {
	return ReplMarshalBytePool{pools: [5]sync.Pool{
		{New: func() any { return []byte{} }},
		{New: func() any { return []byte{} }},
		{New: func() any { return []byte{} }},
		{New: func() any { return []byte{} }},
		{New: func() any { return []byte{} }}}}
}

func (p *ReplMarshalBytePool) Get(wantedLen int) []byte {
	if wantedLen < MIN_REPL_POOL_BUF_SIZE {
		return make([]byte, wantedLen)
	}
	pos := 0                //pos 0: [1KB,10KB[
	if wantedLen < 100000 { //[10KB,100KB[
		pos = 1
	} else if wantedLen < 1000000 { //[100KB,1MB[
		pos = 2
	} else if wantedLen < 10000000 { //[1MB,10MB[
		pos = 3
	} else if wantedLen < 100000000 { //[10MB,100MB[
		pos = 4
	} else { //Buffer too big. Unlikely to be updates, most likely initial data load. Return a new buffer.
		//fmt.Printf("[RG]Allocating a big buffer (%.3f GB). This is unexpected except for initial data loading replication.\n", float64(wantedLen)/1000000000)
		return make([]byte, wantedLen)
	}
	toReturn := p.pools[pos].Get().([]byte)
	if cap(toReturn) < wantedLen {
		return make([]byte, wantedLen, wantedLen+wantedLen/10) //We add a small extra buffer, may be useful for next re-use.
	}
	//We can re-use, perfect. Slice it to wanted size.
	return toReturn[:wantedLen]
}

func (p *ReplMarshalBytePool) Put(buf []byte) {
	if cap(buf) < MIN_REPL_POOL_BUF_SIZE { //Throw out, too small to store.
		return
	}
	if cap(buf) < 10000 { //pos 0: [1KB,10KB[
		p.pools[0].Put(buf)
	} else if cap(buf) < 100000 { //pos 1: [10KB,100KB[
		p.pools[1].Put(buf)
	} else if cap(buf) < 1000000 { //pos 2: [100KB,1MB[
		p.pools[2].Put(buf)
	} else if cap(buf) < 10000000 { //pos 3: [1MB,10MB[
		p.pools[3].Put(buf)
	} else if cap(buf) < 100000000 { //pos 4: [10MB,100MB[
		p.pools[4].Put(buf)
	}
	//else: Ignore, buffer is too big, unlikely to be updates. Most likely initial data load.
}

func (p *ReplMarshalBytePool) PutAll(bufs *tools.SliceWithCounter[[]byte]) {
	var buf []byte
	for i := 0; i < bufs.Len(); i++ {
		buf = bufs.Get(i)
		if cap(buf) < MIN_REPL_POOL_BUF_SIZE { //Throw out, too small to store.
			continue
		}
		if cap(buf) < 10000 { //pos 0: [1KB,10KB[
			p.pools[0].Put(buf)
		} else if cap(buf) < 100000 { //pos 1: [10KB,100KB[
			p.pools[1].Put(buf)
		} else if cap(buf) < 1000000 { //pos 2: [100KB,1MB[
			p.pools[2].Put(buf)
		} else if cap(buf) < 10000000 { //pos 3: [1MB,10MB[
			p.pools[3].Put(buf)
		} else if cap(buf) < 100000000 { //pos 4: [10MB,100MB[
			p.pools[4].Put(buf)
		}
		//else: Ignore, buffer is too big, unlikely to be updates. Most likely initial data load.
	}
	bufs.Clear()
}

//docker run -d --hostname RMQ1 --name rabbitmq1 -p 5672:5672 rabbitmq:latest

func CreateRemoteGroupStruct(bucketsToListen []string, replicaID uint16) (group *RemoteGroup) {
	//myInstanceIP := tools.SharedConfig.GetOrDefault("localRabbitMQAddress", "localhost:5672")
	//othersIPList := strings.Split(tools.SharedConfig.GetConfig("remoteRabbitMQAddresses"), " ")
	if len(othersIPList) == 1 && len(othersIPList[0]) < 2 {
		othersIPList = []string{}
	}
	fmt.Println("[RG]Remote conns:", othersIPList, "(size:", len(othersIPList), ")")

	group = &RemoteGroup{conns: make([]*RemoteConn, len(othersIPList)), nReplicas: uint16(len(othersIPList)), workChan: make(chan RCWork, 100),
		groupChan: make(chan ReplicatorMsg, defaultListenerSize*len(othersIPList)), replicaID: replicaID, knownIPs: make(map[string]uint16)}

	fmt.Printf("[RG]Self ip: %s. Remote ips: %v\n", localRabbitMQIP, othersIPList)
	group.ourConn = CreateRemoteConnStruct(localRabbitMQIP, bucketsToListen, replicaID, math.MaxUint16, true, group.workChan)

	for i, ip := range othersIPList {
		group.conns[i] = CreateRemoteConnStruct(ip, bucketsToListen, replicaID, uint16(i), false, group.workChan)
	}

	group.prepareMsgListener()
	group.prepareWorkerRoutines()
	return
}

// Adds a replica if it isn't already known - a joining replica might be already known e.g. when two new replicas start at the same time, aware of each other.
func (group *RemoteGroup) AddReplica(ip string, bucketsToListen []string, joiningReplicaID uint16) (connID uint16) {
	if id, has := group.knownIPs[ip]; has {
		fmt.Println("Didn't add replica as it is already known.", ip)
		//Already known replica, nothing to do
		return id
	}
	fmt.Println("Start add replica, nReplicas:", group.nReplicas)
	//connChan := make(chan GroupOrErr)
	slot := group.nReplicas
	group.nReplicas += 1
	group.conns = append(group.conns, nil) //Fill "slot" for this connection

	//go connectToIp(ip, int(slot), bucketsToListen, group.replicaID, slot, connChan, false, group.workChan)
	//reply := <-connChan
	//group.conns[slot] = reply.RemoteConn //Update this connection index
	group.conns[slot] = CreateRemoteConnStructWithWait(ip, bucketsToListen, group.replicaID, slot, false, group.workChan)
	group.knownIPs[ip] = slot
	fmt.Println("Finish add replica, nReplicas:", group.nReplicas)
	//go group.listenToRemoteConn(reply.RemoteConn.listenerChan)
	go group.listenToRemoteConn(group.conns[slot].listenerChan)
	return slot
}

/*
func (group *RemoteGroup) SendPartTxn(request *NewReplicatorRequest) {
	group.ourConn.SendPartTxn(request)
}
*/

/*func (group *RemoteGroup) SendGroupTxn(txns []RemoteTxn) {
	group.ourConn.SendGroupTxn(txns)
}

func (group *RemoteGroup) SendTxnsIndividually(txns []RemoteTxn) {
	group.ourConn.SendTxnsIndividually(txns)
}*/

func (group *RemoteGroup) SendTxn(txn RemoteTxn) {
	group.ourConn.SendTxn(txn)
}

func (group *RemoteGroup) SendStableClk(ts int64) {
	group.ourConn.SendStableClk(ts)
}

func (group *RemoteGroup) SendTrigger(trigger AutoUpdate, isGeneric bool) {
	group.ourConn.SendTrigger(trigger, isGeneric)
}

func (group *RemoteGroup) GetNextRemoteRequest() (request ReplicatorMsg) {
	return <-group.groupChan
}

func (group *RemoteGroup) prepareMsgListener() {
	for i := range group.conns {
		go group.listenToRemoteConn(group.conns[i].listenerChan)
	}
	go group.listenToRemoteConn(group.ourConn.listenerChan)
}

func (group *RemoteGroup) listenToRemoteConn(channel chan ReplicatorMsg) {
	for msg := range channel {
		group.groupChan <- msg
	}
}

func (group *RemoteGroup) sendReplicaID(buckets []string, ip string) {
	//Same msg for everyone, so we prepare it here
	protobuf := createProtoRemoteID(group.replicaID, buckets, ip)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, group.replicaID, "Failed to generate bytes of RemoteID msg:", err)
	}
	for _, conn := range group.conns {
		conn.SendRemoteID(data)
	}
}

func (group *RemoteGroup) SendJoin(buckets []string, replicaID uint16) {
	//Same msg for everyone, so we prepare it here
	protobuf := createProtoJoin(buckets, replicaID, localRabbitMQIP)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, group.replicaID, "Failed to generate bytes of Join msg:", err)
	}
	fmt.Println("Sending joins as", replicaID, "to", len(group.conns), "replicas")
	for _, conn := range group.conns {
		conn.SendJoin(data)
	}
}

func (group *RemoteGroup) SendReplyJoin(req ReplyJoin, replicaTo uint16) {
	group.conns[replicaTo].SendReplyJoin(req)
}

func (group *RemoteGroup) SendRequestBucket(req RequestBucket, replicaToIP string) {
	group.conns[group.knownIPs[replicaToIP]].SendRequestBucket(req)
}

func (group *RemoteGroup) SendReplyBucket(req ReplyBucket, replicaToIP string) {
	fmt.Println("Sending ReplyBucket to", replicaToIP)
	group.conns[group.knownIPs[replicaToIP]].SendReplyBucket(req)
}

func (group *RemoteGroup) SendReplyEmpty(replicaTo uint16) {
	group.conns[replicaTo].SendReplyEmpty()
}

func (group *RemoteGroup) prepareWorkerRoutines() {
	//We use minTxnsToGroup as, if more txns than this are sent for replication,
	//they will be grouped in a single proto (and thus single work)
	/*for i := 0; i < minTxnsToGroup; i++ {
		go group.listenForWork()
	}*/
	marshallWorkers := tools.Max(2, tools.Min(20, runtime.NumCPU()/8))
	clkSize := clocksi.GetSliceTimestampSizeForNEntries(len(group.conns) + 1) //+1 for self
	fmt.Printf("[RG]Starting %d worker routines for marshalling. Expected replicas (including self): %d. Clk buffer size: %d bytes.\n", marshallWorkers, len(group.conns)+1, clkSize)
	for i := 0; i < marshallWorkers; i++ {
		go group.listenForWork(clkSize)
	}
}

func (group *RemoteGroup) listenForWork(clkSize int) {
	//For now, ReplMarshallWork is the only work. Later if we have multiple works, we can define a "buffer struct", that holds the re-usable buffer for each work type.
	//Then the work itself implements a get method to know how to get the buffer from that struct.
	//TODO: Maybe I need to cleanup the buffers during GC (not fast GC). But that might be hard given we don't have an individual channel per routine.
	//Also, for byte buffers in particular, I have to be careful as some buffers will ineviatebly be bigger than others. Maybe have to associate the buffer to the kind of work?
	//(In theory we should discard buffers from initial data replication but... there'll be only 1-2 updates per CRDT, as it's bulk updates, so the buffers will actually tend to be too small - so it's okay)
	var work RCWork
	reusableBufs := RCProtoBuffers{RemoteBktTxnProtoBuf: &proto.ProtoReplicateTxn{SenderID: new(int32), Timestamp: make([]byte, clkSize), TxnID: new(int32)}}
	for {
		work = <-group.workChan
		work.DoWork(group.replicaID, reusableBufs)
	}
}

func (work ReplMarshallWork) DoWork(replicaID uint16, buffers RCProtoBuffers) {
	//Optimization notes.
	//Both Marshall and createProtoReplicateTxn are heavy - profiling before createProtoReplicateTxnReuse shows time spent is roughtly 45% createProtoReplicateTxn, 55% marshal.
	//However, Marshall() is expensive due to the cost of traversing the graph/protobuf. In fact, of 70.93s spend on this marshall:
	//- 29.55s spent on marshalAppendPointer
	//- 16.66s spent on checkInitialized (16.43s on checkInitializedPointer)
	//- 23.23s on sizePointer
	//- Only ~1.18s on makeSlice
	//So cost of making the byte buffers is very low. Sadly, it is very complicated to reuse these buffers, as they may be alive for long (until the msg is sent, which is async btw - could be an issue too)
	//And size of buffers may vary a lot between different txns/buckets. It's just hard to keep track. Sync.pool isn't very useful here.
	//For now, I optimized createProtoReplicateTxnReuse, by allowing proto.ProtoReplicateTxn to be re-used (partially, inner updates are still generated new), hopefully that will help.
	//ChatGPT suggests we could compute sizes of the inner protobufs first? Maybe that'll help but I'd need to investigate further.
	//For now I'm content enough that this cost is mostly paid with paralellized CPU time.
	start := time.Now().UnixNano()
	//protobuf := createProtoReplicateTxn(replicaID, work.Txn.Clk, work.Txn.Upds, work.Txn.TxnID)
	//oldSize := buffers.RemoteBktTxnProtoBuf.SizeVT()
	//buffers.RemoteBktTxnProtoBuf = &proto.ProtoReplicateTxn{SenderID: new(int32), Timestamp: make([]byte, clocksi.GetSliceTimestampSize()), TxnID: new(int32)}
	createProtoReplicateTxnReuse(replicaID, work.Txn.Clk, work.Txn.Upds, work.Txn.TxnID, buffers.RemoteBktTxnProtoBuf)
	endProto := time.Now().UnixNano()
	//data, err := pb.Marshal(protobuf)
	//TODO: Change to VT. Figure out also a way to re-use thede data ([]byte) buffers.
	size := buffers.RemoteBktTxnProtoBuf.SizeVT()
	/*if size > 100*1024*1024 {
		fmt.Printf("[RG][DoWork]Warning: Large ProtoReplicateTxn of size %d bytes, old size (before createProtoReplicateTxnReuse) %d bytes, at time %s.\n", size, oldSize, time.Now().Format("2006-01-02 15:04:05.000"))
	}*/
	data := replMarshalByteBufs.Get(size)
	_, err := buffers.RemoteBktTxnProtoBuf.MarshalToSizedBufferVT(data)
	//data, err := pb.Marshal(buffers.RemoteBktTxnProtoBuf)
	endMarshall := time.Now().UnixNano()
	if err != nil {
		fmt.Printf("[RC]Error creating ProtoReplicateTxn (error: %v). Timestamp: %s.\n", err,
			(clocksi.SliceTimestamp{}.FromBytes(buffers.RemoteBktTxnProtoBuf.GetTimestamp())).ToSortedString())
		os.Exit(0)
	}
	work.ReplyChan <- PairKeyBytes{Key: bucketTopicPrefix + work.Bucket, Data: data}
	//fmt.Printf("[RG][DoWork]Replied to replyChan regarding bkt %s, txnID %d.\n", work.Bucket, work.Txn.TxnID)
	work.DebugChan <- StatisticsMarshall{protoCreationTime: endProto - start, marshallTime: endMarshall - endProto}
	//fmt.Printf("[RG][DoWork]Replied to debugChan regarding bkt %s, txnID %d.\n", work.Bucket, work.Txn.TxnID)
}

/*func (work MarshallWork) DoWork(replicaID uint16) {
	//start := time.Now()
	results, i := make([]PairKeyBytes, len(work.BucketOps)), 0
	for bucket, upds := range work.BucketOps {
		protobuf := createProtoReplicateTxn(replicaID, work.Txn.Clk, upds, work.Txn.TxnID)
		//fmt.Printf("[RC][SendTxn]Finished creating protoReplicateTxn. TxnClk: %s. Started at: %s. Current time: %s.\n",
		//work.Txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), time.Now().Format("2006-01-02 15:04:05.000"))
		data, err := pb.Marshal(protobuf)
		//fmt.Printf("[RC][SendTxn]Finished marshalling protoReplicateTxn. TxnClk: %s. Started at: %s. Current time: %s.\n",
		//work.Txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), time.Now().Format("2006-01-02 15:04:05.000"))
		if err != nil {
			//remote.checkProtoError(err, protobuf, upds)
			//fmt.Printf("[RC]Error creating ProtoReplicateTxn (error: %v). Proto: %v. Upds: %v. Timestamp: %s.\n", err, protobuf, upds,
			//(clocksi.ClockSiTimestamp{}.FromBytes(protobuf.GetTimestamp())).ToSortedString())
			fmt.Printf("[RC]Error creating ProtoReplicateTxn (error: %v). Timestamp: %s.\n", err,
				(clocksi.SliceTimestamp{}.FromBytes(protobuf.GetTimestamp())).ToSortedString())
			os.Exit(0)
		}
		results[i] = PairKeyBytes{Key: bucketTopicPrefix + bucket, Data: data}
		i++
		//end := time.Now()
		//fmt.Printf("[RC][SendTxn]Finished sending protoReplicateTxn. TxnClk: %s. Started at: %s. End time: %s. Total time taken: %d\n",
		//work.Txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), (end.UnixNano()-start.UnixNano())/1000000)
	}
	work.ReplyChan <- MarshallWorkReply{Result: results, TxnSendId: work.Txn.TxnID}
}*/

/*func (work GroupMarshallWork) DoWork(replicaID uint16) {
	protobuf := createProtoReplicateGroupTxn(replicaID, work.Txns, work.BktTxns)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		fmt.Printf("[RC]Error marshalling ProtoReplicateGroupTxn (error: %v) proto. Protobuf: %+v\n", err, protobuf)
		os.Exit(0)
	}
	work.ReplyChan <- PairKeyBytes{Key: work.Bucket, Data: data}
}*/
