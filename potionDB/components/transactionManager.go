package components

//TODO (high priority): check where channels can be shared when sending requests to partitions - this would be more efficient
//TODO: Read-write lock for the clock? That might help when doing queries-only.

//A circular array. Each instance adds a read clock (in order) to the array and stores its position
//Then, when the read clears, said position is marked as clear and can be rewritten.
//If the array ever gets full, a new one must be created.
//As such we need to keep the start and end position of said circular array
//Thus it is safe to clean up to exactly the start clock.
//Problem: if we start using the max between client's clock and server's clock, it will no longer work.
//This'll have to be dealt it in some special way...

import (
	fmt "fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"potionDB/potionDB/utilities"
	"potionDB/shared/shared"

	"github.com/AndreRijo/go-tools/src/tools"
)

/////*****************TYPE DEFINITIONS***********************/////
//TODO: Extract requests types, replies and methods to another file

/*type KeyParams struct {
	Key      string
	CrdtType proto.CRDTType
	Bucket   string
}

type UpdateObjectParams struct {
	KeyParams
	UpdateArgs *crdt.UpdateArguments
}

type ReadObjectParams struct {
	KeyParams
	ReadArgs crdt.ReadArguments
}*/

type TransactionManagerRequest struct {
	TransactionId //TODO: Remove this, as most requests don't need it (iirc, only staticWrite, commit and abort use it)
	Timestamp     clocksi.Timestamp
	Args          TMRequestArgs
}

type TMRequestArgs interface {
	getRequestType() (requestType TMRequestType)
}

// For now, only used in S2S. Clients from ProtoServer will still issue static/non-static reads, and TM identifies when a read should be single.
type TMSingleReadArgs struct {
	ReadParams crdt.ReadObjectParams
	ReplyChan  chan TMStaticReadReply
}

type TMReadArgs struct {
	ReadParams     []crdt.ReadObjectParams
	ProcReadParams []crdt.ReadProcessingObjectParams
	ReplyChan      chan []crdt.State
}

type TMUpdateArgs struct {
	UpdateParams []crdt.UpdateObjectParams
	ReplyChan    chan TMUpdateReply
}

type TMStaticUpdateArgs struct {
	UpdateParams []crdt.UpdateObjectParams
	ReplyChan    chan TMStaticUpdateReply
}

// Used for setting the initial database state. Only accessible through internal client. It sends updates directly to Materializer, without any coordination or timestamp management.
type TMInitialDataArgs struct {
	UpdateParams []crdt.UpdateObjectParams
	ReplyChan    chan bool //In this case, it does not make sense to share the Timestamp/TransactionID.
}

type TMStaticReadArgs struct {
	ReadParams     []crdt.ReadObjectParams
	ProcReadParams []crdt.ReadProcessingObjectParams
	ReplyChan      chan TMStaticReadReply
}

type TMConnLostArgs struct {
}

type TMStartTxnArgs struct {
	ReplyChan chan TMStartTxnReply
}

type TMCommitArgs struct {
	ReplyChan chan TMCommitReply
}

type TMAbortArgs struct {
}

type TMNewTriggerArgs struct {
	Source, Target Link
	IsGeneric      bool
	ReplyChan      chan bool
}

type TMGetTriggersArgs struct {
	WaitFor   chan bool
	ReplyChan chan *TriggerDB
}

type TMManualGCArgs struct {
	ReplyChan chan bool
}

type TMS2SRequest struct {
	ClientID uint64
	Args     TMRequestArgs
}

type TMS2SReply struct {
	ClientID  uint64
	TxnID     TransactionId
	ReplyType proto.WrapperType
	Reply     interface{}
}

type TMServerConn struct {
	ReplicaID uint16
	ReplyChan chan TMS2SReply
	ReqChan   chan TransactionManagerRequest //Optional. This is used in case of connections that get upgraded from normal to S2S.
}

type TMBCPermsArgs struct {
	Perms        []map[crdt.KeyParams]int32
	ReqReplicaID uint16
}

type TMMultiClientReply struct {
	ClientID int
	TxnId    TransactionId
	Reply    interface{}
}

/*****Remote/Replicator interaction structs*****/

//Used by the Replication Layer. Use a different thread to handle this
/*
type TMRemoteTxn struct {
	ReplicaID uint16
	Upds      []NewRemoteTxns
	StableTs  int64
}
*/

type TMRemoteMsg interface {
	getReplicaID() uint16
}

type TMRemoteClk struct {
	ReplicaID uint16
	StableTs  int64
}

type TMGetSnapshot struct {
	Buckets   map[string]struct{}
	ReplyChan chan TMGetSnapshotReply
}

type TMApplySnapshot struct {
	clocksi.Timestamp
	PartStates [][]*proto.ProtoCRDT
}

// This one is both Remote/Replicator related but also S2S.
type TMReplicaID struct {
	ReplicaID uint16
	IP        string
	Buckets   []string
	ReplyChan chan TMS2SReply
	ReqChan   chan TransactionManagerRequest //Optional. This is used in case of connections that get upgraded from normal to S2S.
}

type TMRemoteTrigger struct {
	AutoUpdate
	IsGeneric bool
}

type TMStart struct {
}

/*****Msgs for handling ops generated by downstream remotes*****/

type TMDownstreamRemoteMsg interface {
}

type TMTxnForRemote struct {
	ops map[uint64][]crdt.UpdateObjectParams
}

type TMMultipleTxnForRemote struct {
	ops map[uint64][][]crdt.UpdateObjectParams
}

type TMNewRemoteTxn struct {
	clocksi.Timestamp
	nPartitions int
}

type TMDownstreamNewOps struct {
	clocksi.Timestamp
	partitionID int64
	newOps      []crdt.UpdateObjectParams
}

/***** *****/

type TMStaticReadReply struct {
	States    []crdt.State
	Timestamp clocksi.Timestamp
}

type TMStaticUpdateReply struct {
	TransactionId
	Timestamp clocksi.Timestamp
	Err       error
}

type TMUpdateReply struct {
	Success bool
	Err     error
}

type TMStartTxnReply struct {
	TransactionId
	Timestamp clocksi.Timestamp
}

type TMCommitReply struct {
	Timestamp clocksi.Timestamp
	Err       error
}

type TMGetSnapshotReply struct {
	Timestamp  clocksi.Timestamp
	PartStates [][]*proto.ProtoCRDT
}

type TMNewTriggerReply struct{}

type TMGetTriggersReply struct {
}

/***** Messages for debugging/testing. May provide unsafe functionality!*****/

type TMGetCRDTArgs struct {
	KeyParams []crdt.KeyParams
	ReplyChan chan []crdt.CRDT
}

type TMGetCRDTReply struct {
	CRDTs []*crdt.CRDT
}

//

type PotionDBStatus int

type TMRequestType int

type ClientId uint64

type TransactionId uint64

type ongoingRemote struct {
	originalClk  clocksi.Timestamp
	txnDataToUse [][]byte
	nTxnsStarted int              //Number of connections with a txn started (txnDataToUse[i] != nil). When it's equal to len(conns), can skip a check to start txn.
	lockChans    []chan msgToSend //Channels to talk with other servers on non-static transactions
	replyChans   []chan msgReply
}

type ongoingTxn struct {
	TransactionId
	//partSet
	//partitions []bool //true: partition participates; false: partition doesn't participate.
	partitions           tools.BitSet
	prepInfoPerPartition []tools.Pair[int32, clocksi.Timestamp] //Index: partitionID. When updating, we hold for each partition the position of the txn in mat's prepClks structure. This avoids map overhead.
	debugID              int                                    //random ID just for debbuging purposes
	//conns      []net.Conn //connection to other replicas that have been created by this transaction
	ongoingRemote
}

// Used by each client goroutine in TM to hold re-usable buffers for updating, avoiding constant allocation during updates
// Buffers inside are initialized on the first time clients do an update.
type tmUpdBuffers struct {
	//replyChan            chan clocksi.Timestamp
	replyChan            chan tools.Triple[int16, int32, clocksi.Timestamp] //TM holds some information for materializer regarding the prepare, to avoid the usage of maps by txnID.
	updsPerPartition     []tools.SliceWithCounter[crdt.UpdateObjectParams]  //Index: partitionID.
	reqsPerServer        []tools.SliceWithCounter[crdt.UpdateObjectParams]  //Index: serverIndex.
	partitionBitSet      tools.BitSet
	prepInfoPerPartition []tools.Pair[int32, clocksi.Timestamp] //Index: partitionID. When updating, we hold for each partition the position of the txn in mat's prepClks structure. This avoids map overhead.
}

type tmReadBuffers struct {
	readChan         chan tools.Pair[int, crdt.State]
	states           []crdt.State
	singleReadChan   chan StateClockPair //For single reads
	singleReadStates []crdt.State
}

func (rBuf *tmReadBuffers) Init() *tmReadBuffers {
	//It may seem silly to start with size of 1, but it's better than a new alloc every iteration, and for many queries, one is enough.
	//It'll grow up as needed, after a few queries it should no longer need to grow.
	rBuf.readChan, rBuf.states = make(chan tools.Pair[int, crdt.State], 1), make([]crdt.State, 1)
	rBuf.singleReadChan = make(chan StateClockPair, 1)
	rBuf.singleReadStates = make([]crdt.State, 1)
	return rBuf
}

//type partSet map[uint64]struct{}

// We use atomics, as they're much faster than sync.Mutex. We focus on read performance, as we can also reduce write frequency if needed.
// Idea: two buffers. Readers always read from the buf pointed by readPtr. The writter writes to the other buffer, and then swaps the pointer.
/*type ProtectedClock struct {
	bufA    clocksi.Timestamp
	bufB    clocksi.Timestamp
	readPtr atomic.Pointer[clocksi.Timestamp]
}*/

// We use atomics, as they're much faster than sync.Mutex. We focus on read performance, as we can also reduce write frequency if needed.
// We can't use the idea of two (or more) buffers, as then readers would be forced to copy the clock and, even then, it's not safe (a reader could get the Pointer, then block for a long time before the Copy finishes, leading to an incorrect read)
// Furthermore, forcing readers to always copy is non-optimal.
// So, instead, reads are direct and writting will always allocate a new clock. This is preferred, as not only it is safe, we also reduce allocation rate.
// Note that our writter attempts to compact multiple local updates into a single update, thus reducing write pressure.
type ProtectedClock struct {
	readPtr atomic.Pointer[clocksi.Timestamp]
}

/*
	type ProtectedClock struct {
		clocksi.SliceTimestamp
		sync.Mutex
	}
*/
/*type ProtectedClock struct {
	clocksi.Timestamp
	sync.Mutex
}*/

type ProtectedTriggerDB struct {
	TriggerDB
	sync.RWMutex
}

type TransactionManager struct {
	mat              *Materializer
	gc               *GarbageCollector
	remoteChan       chan TMRemoteMsg
	localClock       ProtectedClock
	txnsSinceCompact int //Number of txns done since the last time history was compacted. Also protected by the above mutex.
	//downstreamQueue  map[uint16][]TMRemoteMsg
	downstreamQueue []tools.SliceWithHideable[TMRemoteMsg]
	inDownQueue     int //Counts how many TMRemoteMsg in the queue above.
	replicator      *Replicator
	replicaID       uint16
	downstreamOpsCh chan TMTxnForRemote //Channel for handling ops that are generated when applying remote downstreams.
	waitStartChan   chan PotionDBStatus //Channel for notifying ProtoServer when is TM ready to start processing requests
	triggerDB       ProtectedTriggerDB
	//Only used if doCompactHistory=true
	ongoingReads map[TransactionId]int //TxnID -> position in circular array
	clocksArray  *utilities.CircularArray
	RemoteInfo
	connPool   *connPool
	commitChan chan TMCommitInfo //Clock updates go to this channel. When a client needs to wait for a clock, the request also goes here
	//commitChan *zenq.ZenQ[TMCommitInfo]
	TMIdsInfo
	//TxnStartTime map[TransactionId]int64 //TODO: Delete, only for debug
	replicaIDs           []uint16
	matRemoteUpdsChan    chan tools.Pair[int64, []crdt.UpdateObjectParams] //Chan to receive extra upds generated by applying remote upds on NuCRDTs.
	bufPendingRemoteTxns []tools.SliceWithHideable[MatRemoteTxn]           //Buffer used by checkPendingRemoteTxns, to temporarely hold txns that are ready to be sent to the partitions. Index is partitionID.
	//bufTxnsToApply       []tools.SliceWithCounter[[]crdt.UpdateObjectParams] //Buffer used by handleRemoteMsgs() to group incoming txns, for more efficient processing.
	bufTxnsToApply []tools.SliceWithCounter[MatRemoteTxn] //Buffer used by handleRemoteMsgs() to group incoming txns, for more efficient processing.
	remoteClock    clocksi.Timestamp                      //The goroutine that handles remote messages keeps its own updated clock. This prevents remote txns from blocking/being queued due to localClock not having been updated yet.
	//Debug counters.
	localTxnsProc     int
	remoteTxnsProc    int  //Remote txns that we have received
	remoteReqsApplied int  //Remote requests as seen by the Materializer, i.e., a group of txns counts as one request.
	anyUpdatesSinceGC bool //Helper variable to decide if a GC is needed as not when PotionDB appears to be idle. While idle, localClock still advances due to remote TMRemoteClks.
	nEverQueued       int  //Counts how many times a remote request got queued due to missing/behind clock.
}

type RemoteInfo struct {
	remoteBks       [][]string //Note: This gets turned to nil after all replicas are known
	remoteIPs       []string
	bucketToIndex   map[string][]int
	remoteIDToIndex map[uint16]int
	ownBuckets      []string
	hasAll          bool //In case server is replicating "*"
	sync.Mutex           //This lock is needed as with S2S, we may receive replicaIDs concurrently and concurrently write to remoteIDToIndex
}

type TMClientID uint64 //Highest bit is a boolean stating if this is a re-used ID. This is important to avoid incrementing maxIDInUse.

func (tmId TMClientID) GetId() int { return int(tmId & (0x7FFFFFFFFFFFFFFF)) }

func (tmId TMClientID) IsReused() bool { return (tmId&(1<<63) != 0) }

type TMIdsInfo struct {
	clksInUse      []clocksi.Timestamp //For each client, contains the read clock of the ongoing txn
	maxIDInUse     int64               //(atomic int) The last position in clksInUse that is relevant
	newIDChan      chan TMClientID     //Channel from which new clients get their TM's IDs. //TODO: Some way to reset this safely.
	canReuseIDChan chan int            //When a connection closes, the ID must be sent to this channel for later re-use
}

// Contains re-usable buffers of each client instance
type TMClientBuffers struct {
	readChans        []chan crdt.State
	states           []crdt.State
	reqsPerServer    [][]crdt.ReadObjectParams
	remoteReqsToChan [][]int
}

type ClockHeap struct {
	entries  []TMWaitClock
	nEntries *int
}

type TMCommitInfo interface{}

// Sent by each partition of the materializer
type TMPartCommitReply struct {
	txnId TransactionId
}

// Sent by the goroutine who asked for the commit
type TMCommitNPartitions struct {
	nPartitions int
	txnId       TransactionId
	clk         clocksi.Timestamp
}

// Used by remote clk, so that the commitChan routine knows there may not have been any update associated to the new clk.
type TMCommitReplClkOnly struct {
	stableTs        int64
	sortedReplicaID uint16
}

// Used by remote txns.
type TMCommitReplClk struct {
	stableTs        int64
	sortedReplicaID uint16
}

// Issued by checkPendingRemoteTxns, as in that situation we may update several positions.
type TMCommitReplFullClk struct {
	clk clocksi.Timestamp
}

/*type TMCommitReplTxn struct {
	Clk       clocksi.Timestamp
	replicaID uint16
}*/

// Message sent by StaticRead() and StartTransaction() when the client's clock is too new.
type TMWaitClock struct {
	targetClk clocksi.Timestamp
	replyChan chan clocksi.Timestamp //Replies with the actual clock of TM
}

// Used to get a copy of the TM's current clock
type TMGetClock struct {
	replyChan chan clocksi.Timestamp
}

type TM_CLIENT_TYPE byte //TM_NORMAL_CLIENT, TM_SERVER_CLIENT, TM_INTERNAL_CLIENT.

/////*****************CONSTANTS AND VARIABLES***********************/////

const (
	readStaticTMRequest    TMRequestType = 0
	updateStaticTMRequest  TMRequestType = 1
	readTMRequest          TMRequestType = 2
	updateTMRequest        TMRequestType = 3
	startTxnTMRequest      TMRequestType = 4
	commitTMRequest        TMRequestType = 5
	abortTMRequest         TMRequestType = 6
	newTriggerTMRequest    TMRequestType = 7
	getTriggersTMRequest   TMRequestType = 8
	bcPermsTMRequest       TMRequestType = 9
	getCRDTTMRequest       TMRequestType = 10
	readSingleTMRequest    TMRequestType = 11
	initialDataTMRequest   TMRequestType = 12
	manualGCTMRequest      TMRequestType = 13
	serverConnRequest      TMRequestType = 80
	serverReplicaIDRequest TMRequestType = 81
	lostConnRequest        TMRequestType = 255

	downstreamOpsChBufferSize int = 100 //Default buffer size for the downstreamOpsCh
	DOWN_QUEUE_STARTING_LEN   int = 20
	TM_MAX_TXN_MERGE          int = 50

	TM_READY, REPL_READY, BOTH_READY                       = PotionDBStatus(1), PotionDBStatus(2), PotionDBStatus(3)
	TM_NORMAL_CLIENT, TM_SERVER_CLIENT, TM_INTERNAL_CLIENT = TM_CLIENT_TYPE(0), TM_CLIENT_TYPE(1), TM_CLIENT_TYPE(2)
)

// Both are filled from configs
var (
	doCompactHistory         = false
	historyCompactInterval   = 60
	historyCompactTargetTxns = 1000
	circularArraySize        = 1000  //Array grows as needed
	FAST_SINGLE_READ         = false //If true, when a static read is issued for a single CRDT, it skips checking the TM's clock, avoiding the lock.
)

/////*****************TYPE METHODS***********************/////

//TransactionManagerRequest

func (args TMStaticReadArgs) getRequestType() (requestType TMRequestType) { return readStaticTMRequest }
func (args TMStaticUpdateArgs) getRequestType() (requestType TMRequestType) {
	return updateStaticTMRequest
}
func (args TMInitialDataArgs) getRequestType() (requestType TMRequestType) {
	return initialDataTMRequest
}
func (args TMReadArgs) getRequestType() (requestType TMRequestType)       { return readTMRequest }
func (args TMSingleReadArgs) getRequestType() (requestType TMRequestType) { return readSingleTMRequest }
func (args TMUpdateArgs) getRequestType() (requestType TMRequestType)     { return updateTMRequest }
func (args TMConnLostArgs) getRequestType() (requestType TMRequestType)   { return lostConnRequest }
func (args TMStartTxnArgs) getRequestType() (requestType TMRequestType)   { return startTxnTMRequest }
func (args TMCommitArgs) getRequestType() (requestType TMRequestType)     { return commitTMRequest }
func (args TMAbortArgs) getRequestType() (requestType TMRequestType)      { return abortTMRequest }
func (args TMNewTriggerArgs) getRequestType() (requestType TMRequestType) { return newTriggerTMRequest }
func (args TMGetTriggersArgs) getRequestType() (requestType TMRequestType) {
	return getTriggersTMRequest
}
func (args TMManualGCArgs) getRequestType() (requestType TMRequestType) { return manualGCTMRequest }
func (args TMServerConn) getRequestType() (requestType TMRequestType)   { return serverConnRequest }
func (args TMS2SRequest) getRequestType() (requestType TMRequestType) {
	return args.Args.getRequestType()
}
func (args TMBCPermsArgs) getRequestType() (requestType TMRequestType) { return bcPermsTMRequest }
func (args TMGetCRDTArgs) getRequestType() (requestType TMRequestType) { return getCRDTTMRequest }
func (args TMReplicaID) getRequestType() (requestType TMRequestType)   { return serverReplicaIDRequest }

//TMRemoteMsg

// RemoteTxn and RemoteTxnGroup are shared with replicator
func (req RemoteTxn) getReplicaID() (id uint16) { return req.SenderID }

// func (req RemoteTxnGroup) getReplicaID() (id uint16)   { return req.SenderID }
func (req TMRemoteClk) getReplicaID() (id uint16)      { return req.ReplicaID }
func (args TMGetSnapshot) getReplicaID() (id uint16)   { return 0 } //Irrelevant
func (args TMApplySnapshot) getReplicaID() (id uint16) { return 0 } //Irrelevant
func (args TMStart) getReplicaID() (id uint16)         { return 0 } //Irrelevant
func (args TMReplicaID) getReplicaID() (id uint16)     { return args.ReplicaID }
func (args TMRemoteTrigger) getReplicaID() (id uint16) { return 0 } //Irrelevant

/*func (req RemoteTxnGroup) getMinClk() (clk clocksi.Timestamp) {
	return req.Txns[0].Clk
}
func (req RemoteTxnGroup) getMaxClk() (clk clocksi.Timestamp) {
	return req.Txns[len(req.Txns)-1].Clk
}*/

//Others

/*
func makePartSet() (set partSet) {
	set = partSet(make(map[uint64]struct{}))
	return
}

func (set partSet) add(partId uint64) {
	set[partId] = struct{}{}
}
*/

func (txnPartitions *ongoingTxn) reset() {
	txnPartitions.TransactionId = 0
	//txnPartitions.partSet = nil
	//txnPartitions.partitions = make([]bool, nGoRoutines)
	//clear(txnPartitions.partitions)
	txnPartitions.partitions.Reset()
	//txnPartitions.ongoingRemote = ongoingRemote{}
	txnPartitions.ongoingRemote.reset()
	clear(txnPartitions.prepInfoPerPartition) //This can be commented out for performance reasons. Its only usage is to allow GC of the last clocks stored here.
}

func (remote *ongoingRemote) reset() {
	remote.originalClk, remote.txnDataToUse, remote.nTxnsStarted = nil, nil, 0
}

func (buf *tmUpdBuffers) resetBufsExceptBitset() {
	for i := range buf.updsPerPartition {
		buf.updsPerPartition[i].Clear()
	}
	for i := range buf.reqsPerServer {
		buf.reqsPerServer[i].Clear()
	}
	//clear(buf.prepInfoPerPartition) //Optional, its only usage is to allow GC of the clocks stored here.
}

func (buf *tmUpdBuffers) reset() {
	buf.resetBufsExceptBitset()
	buf.partitionBitSet.Reset()
}

func TMWaitClockLess(a, b TMWaitClock) bool {
	return a.targetClk.IsLowerOrEqualTotalOrder(b.targetClk)
}

func (c ClockHeap) Len() int { return *c.nEntries }

// We want the heap's Pop() to return the lowest element, so we use < on "less". The smallest element is on h[0].
func (c ClockHeap) Less(i, j int) bool {
	return c.entries[i].targetClk.IsLowerOrEqualTotalOrder(c.entries[j].targetClk)
}

func (c ClockHeap) Swap(i, j int) {
	c.entries[i], c.entries[j] = c.entries[j], c.entries[i]
}

func (c ClockHeap) Push(value interface{}) {
	convValue := value.(TMWaitClock)
	if *c.nEntries == cap(c.entries) {
		c.entries = append(c.entries, convValue)
		c.entries = c.entries[:cap(c.entries)] //Extending to capacity
		*c.nEntries += 1
	} else {
		c.entries[*c.nEntries], *c.nEntries = convValue, *c.nEntries+1
	}

}

func (c ClockHeap) Pop() interface{} {
	if *c.nEntries == 0 {
		return nil
	}
	old := c.entries[*c.nEntries-1]
	c.entries[*c.nEntries-1] = TMWaitClock{}
	*c.nEntries--
	return old
}

// Returns the lowest value, but does not remove it.
func (c ClockHeap) PeekMin() TMWaitClock {
	if *c.nEntries == 0 {
		return TMWaitClock{}
	}
	return c.entries[0]
}

/*type ProtectedClock struct {
	readPtr atomic.Pointer[clocksi.Timestamp]
}*/

func (pc *ProtectedClock) GetClock() clocksi.Timestamp {
	return *pc.readPtr.Load()
}

func (pc *ProtectedClock) GetValue(replicaID uint16) int64 {
	return (*pc.readPtr.Load()).GetPos(replicaID)
}

// No need to check if the value is higher, as the writer routine already ensures that.
func (pc *ProtectedClock) UpdatePos(replicaID uint16, value int64) {
	clk := *pc.readPtr.Load()
	newClk := clk.Copy()
	newClk.UpdatePos(replicaID, value)
	pc.readPtr.Store(&newClk)
}

func (pc *ProtectedClock) UpdateTwoPos(replicaID1 uint16, value1 int64, replicaID2 uint16, value2 int64) {
	clk := *pc.readPtr.Load()
	newClk := clk.Copy()
	newClk.UpdatePos(replicaID1, value1)
	newClk.UpdatePos(replicaID2, value2)
	pc.readPtr.Store(&newClk)
}

func (pc *ProtectedClock) Update(otherClk clocksi.Timestamp) {
	clk := *pc.readPtr.Load()
	newClk := clk.Merge(otherClk) //This returns a new clock, so it's safe.
	pc.readPtr.Store(&newClk)
}

/////*****************TRANSACTION MANAGER CODE***********************/////

// Closes S2S connections and any other resources in preparation for server shut down.
func (tm *TransactionManager) ShutDown() {
	tm.connPool.closeConnections()
	fmt.Println("[TM]S2S connections closed. TM ready for shutdown.")
}

func (tm *TransactionManager) ResetServer() {
	matChan := make(chan bool, len(tm.mat.channels))
	tm.mat.SendRequestToAllChannels(MaterializerRequest{MatRequestArgs: MatResetArgs{ReplyChan: matChan}})
	tm.replicator.Reset()
	tm.remoteChan = make(chan TMRemoteMsg)
	//tm.localClock = ProtectedClock{Mutex: sync.Mutex{}, SliceTimestamp: clocksi.NewSliceTimestamp()}
	//tm.downstreamQueue = make(map[uint16][]TMRemoteMsg)
	for i := range tm.downstreamQueue {
		tm.downstreamQueue[i].DeepClear()
	}
	for i := range tm.bufPendingRemoteTxns {
		tm.bufPendingRemoteTxns[i].DeepClear()
	}
	for i := range tm.bufTxnsToApply {
		tm.bufTxnsToApply[i].DeepClear()
	}
	for i := 0; i < len(tm.mat.channels); i++ {
		<-matChan
	}
	fmt.Println("[TM]Reset complete.")
}

func Initialize(replicaID uint16, initialDataLoad bool) (tm *TransactionManager) {
	setConfigs()
	buckets := shared.Buckets
	clocksi.AddNewID(replicaID)
	downstreamOpsCh := make(chan TMTxnForRemote, downstreamOpsChBufferSize)
	commitCh := make(chan TMCommitInfo, 2000)
	//commitCh := zenq.New[TMCommitInfo](1024) //Power of 2.
	mat, loggers := InitializeMaterializer(replicaID, commitCh)
	//mat, _, _ := InitializeMaterializer(replicaID)
	tm = &TransactionManager{
		mat:        mat,
		remoteChan: make(chan TMRemoteMsg, 500), //TODO: Make this size a variable too?
		//localClock:       ProtectedClock{Mutex: sync.Mutex{}, SliceTimestamp: clocksi.NewSliceTimestamp()},
		//localClock:       ProtectedClock{Mutex: sync.Mutex{}, Timestamp: clocksi.NewSliceTimestamp()},
		txnsSinceCompact: 0,
		//downstreamQueue:  make(map[uint16][]TMRemoteMsg),
		replicator:      &Replicator{},
		replicaID:       replicaID,
		downstreamOpsCh: downstreamOpsCh,
		waitStartChan:   make(chan PotionDBStatus, 2), //2: msg from TM and msg from Replicator (forwarded by TM)
		triggerDB:       ProtectedTriggerDB{RWMutex: sync.RWMutex{}, TriggerDB: InitializeTriggerDB()},
		RemoteInfo:      RemoteInfo{bucketToIndex: make(map[string][]int), remoteIDToIndex: make(map[uint16]int), hasAll: false},
		commitChan:      commitCh,
		TMIdsInfo: TMIdsInfo{
			clksInUse:      make([]clocksi.Timestamp, 100),
			maxIDInUse:     0,
			newIDChan:      make(chan TMClientID, 10),
			canReuseIDChan: make(chan int, 100),
		},
		matRemoteUpdsChan: make(chan tools.Pair[int64, []crdt.UpdateObjectParams], nGoRoutines),
		//TxnStartTime: make(map[TransactionId]int64),
	}
	tm.localClock.readPtr.Store(&clocksi.EmptyTs)
	tm.bufPendingRemoteTxns = make([]tools.SliceWithHideable[MatRemoteTxn], nGoRoutines)
	//tm.bufTxnsToApply = make([]tools.SliceWithCounter[[]crdt.UpdateObjectParams], nGoRoutines)
	tm.bufTxnsToApply = make([]tools.SliceWithCounter[MatRemoteTxn], nGoRoutines)
	for i := uint64(0); i < nGoRoutines; i++ {
		tm.bufPendingRemoteTxns[i] = tools.NewSliceWithHideable[MatRemoteTxn](DOWN_QUEUE_STARTING_LEN)
		tm.bufTxnsToApply[i] = tools.NewSliceWithCounter[MatRemoteTxn](TM_MAX_TXN_MERGE)
		//tm.bufTxnsToApply[i] = tools.NewSliceWithCounter[[]crdt.UpdateObjectParams](DOWN_QUEUE_STARTING_LEN)
	}
	tm.ownBuckets = buckets
	//Check if server replicates all buckets
	for _, bkt := range tm.ownBuckets {
		if bkt == "*" {
			tm.hasAll = true
			break
		}
	}

	if !doesJoin {
		fmt.Printf("[TM]Initializing connPool directly from TM's initialization at %s.\n", time.Now().Format("15:04:05.000"))
		tm.connPool = optimisticInitializeConnPool(othersIPList) //OK, non-blocking.
	} else {
		fmt.Printf("[TM]DoesJoin is true! Thus, connPool not started during TM's initialization.")
	}

	if doCompactHistory {
		tm.ongoingReads, tm.clocksArray = make(map[TransactionId]int), &utilities.CircularArray{}
		tm.clocksArray.Initialize(circularArraySize)
		//go tm.doHistoryCompact(waitRoutinesStart)		//Not implemented.
	}
	//go tm.replicator.Initialize(tm, loggers, buckets, replicaID, initialDataLoad)
	tm.replicator.Initialize(tm, loggers, buckets, replicaID, initialDataLoad)
	//go tm.handleCommitReplies() //We now start it only after TMStart{}, so that we can obtain an initial copy of the clock already with all replicas.
	go tm.handleRemoteMsgs()
	nDownGenHandlers := 4 //TODO: Put this as some variable that can be configured.
	for i := 0; i < nDownGenHandlers; i++ {
		go tm.handleDownstreamGeneratedOps()
	}
	go tm.generateTMIDs()
	if debugMode {
		go tm.sanityCheck()
	}
	tm.gc = InitializeGarbageCollector(tm)
	//tm.gc.StartGCTimer()

	//All the extra goroutines are quick to initialize (i.e., they are all fors that go forever and don't have much preparatory work)
	//PotionDB will sleep for a short to while to give an oportunity for everything to initialize.

	//Debug, remove
	go func() {
		for {
			time.Sleep(30 * time.Second)
			//fmt.Printf("[TM]LocalTxns: %d. Remote txns: %d. TM clk: %s\n", tm.localTxnsProc, tm.remoteTxnsProc, tm.localClock.GetClock().ToString())
			fmt.Printf("[TM]LocalTxns: %d. Remote reqs applied: %d. Remote txns received: %d. TM clk: %s.\n", tm.localTxnsProc, tm.remoteReqsApplied, tm.remoteTxnsProc, tm.localClock.GetClock().ToString())
		}
	}()

	fmt.Printf("[TM]Finished initialization.\n")
	return tm
}

func setConfigs() {
	//TM
	fmt.Println("[TM]Default TopKSize defined in configs:", tools.SharedConfig.GetIntConfig("topKSize", 100))
	crdt.SetTopKSize(tools.SharedConfig.GetIntConfig("topKSize", 100))
	FAST_SINGLE_READ = tools.SharedConfig.GetBoolConfig("fastSingleRead", false)
	getBucketsFromConfig()
	//MAT
	nGoRoutines = uint64(tools.SharedConfig.GetIntConfig("nPartitions", 1))
	nGoRoutines = 64 //TODO: REMOVE.
	requestQueueSize = tools.SharedConfig.GetIntConfig("requestChannelSize", 50)
	expectedNewDownstreamSize = tools.SharedConfig.GetIntConfig("newDownstreamSize", 10)
	keyRangeSize = math.MaxUint64/nGoRoutines + 1 //We add +1 to force all hashes to be < nGoRoutines.
	//RC
	basePrefix = tools.SharedConfig.GetOrDefault("rabbitMQUser", "guest")
	baseVHost = tools.SharedConfig.GetOrDefault("rabbitVHost", "/crdts")
	//Replicator
	doesJoin = tools.SharedConfig.GetBoolConfig(DO_JOIN, true)
	localPotionIP = tools.SharedConfig.GetOrDefault("localPotionDBAddress", "localhost:8087")
	localRabbitMQIP = tools.SharedConfig.GetOrDefault("localRabbitMQAddress", "localhost:5672")
	//RemoteGroup
	othersIPList = strings.Split(tools.SharedConfig.GetConfig("remoteRabbitMQAddresses"), " ")
	//CP
	MAX_POOL_PER_SERVER = int64(tools.SharedConfig.GetIntConfig("poolMax", 100))
}

func getBucketsFromConfig() {
	stringBuckets, has := tools.SharedConfig.GetAndHasConfig("buckets")
	if !has {
		shared.Buckets = []string{"*"}
	} else {
		shared.Buckets = strings.Split(stringBuckets, " ")
	}
}

func (tm *TransactionManager) WaitUntilReady() PotionDBStatus {
	return <-tm.waitStartChan
}

// Starts a goroutine to handle the client requests. Returns a channel to communicate with that goroutine
// func (tm *TransactionManager) CreateClientHandler(clientType TM_CLIENT_TYPE) (channel chan TransactionManagerRequest) {
func (tm *TransactionManager) CreateClientHandler() (channel chan TransactionManagerRequest) {
	channel = make(chan TransactionManagerRequest)
	id := <-tm.newIDChan
	//atomic.AddInt64(&tm.maxIDInUse, 1)
	if !id.IsReused() {
		atomic.AddInt64(&tm.maxIDInUse, 1)
	}
	go tm.listenForProtobufRequests(channel, id.GetId())
	return
}

// This one is used when we know from the beginning the connection will be of S2S. It creates a channel that can receive multiple requests.
func (tm *TransactionManager) CreateClientS2SHandler() (reqChan chan TransactionManagerRequest, replyChan chan TMS2SReply) {
	reqChan, replyChan = make(chan TransactionManagerRequest, S2S_TM_CHAN_SIZE), make(chan TMS2SReply, S2S_TM_CHAN_SIZE)
	id := <-tm.newIDChan
	//atomic.AddInt64(&tm.maxIDInUse, 1)
	if !id.IsReused() {
		atomic.AddInt64(&tm.maxIDInUse, 1)
	}
	go tm.listenForProtobufRequests(reqChan, id.GetId()) //TODO: Maybe different method.
	return
}

// Used when upgrading a client handler to S2S. The caller informs TM of the new reqChan through TMReplicaID or TMServerConn requests.
func (tm *TransactionManager) MakeS2SReqChan() (reqChan chan TransactionManagerRequest) {
	return make(chan TransactionManagerRequest, S2S_TM_CHAN_SIZE)
}

// Receives an existing handler, kills the goroutine and starts a different kind of listener for each client.
func (tm *TransactionManager) UpgradeHandlerToMultiClient(channel chan TransactionManagerRequest, nClients int) (channels []chan TransactionManagerRequest, replyChan chan TMMultiClientReply) {
	channel <- TransactionManagerRequest{Args: TMConnLostArgs{}}
	channels, replyChan = make([]chan TransactionManagerRequest, nClients), make(chan TMMultiClientReply, nClients)
	for i := 0; i < nClients; i++ {
		channels[i] = make(chan TransactionManagerRequest, 1)
		id := <-tm.newIDChan
		//atomic.AddInt64(&tm.maxIDInUse, 1)
		if !id.IsReused() {
			atomic.AddInt64(&tm.maxIDInUse, 1)
		}
		go tm.listenForProtobufRequestMultiClient(channels[i], id.GetId(), int(i), replyChan)
	}
	return
}

func (tm *TransactionManager) SendRemoteMsg(msg TMRemoteMsg) {
	tm.remoteChan <- msg
}

func (tm *TransactionManager) listenForProtobufRequests(channel chan TransactionManagerRequest, id int) {
	//stop := false
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	//rng := rand.New(rand.NewSource(int64(id)))
	var txnPartitions *ongoingTxn = &ongoingTxn{}
	//txnPartitions.partitions = make([]bool, nGoRoutines)
	txnPartitions.partitions = tools.NewBitSet(int(nGoRoutines))
	txnPartitions.debugID = rng.Intn(10)
	txnPartitions.ongoingRemote = ongoingRemote{}
	txnPartitions.lockChans = make([]chan msgToSend, len(tm.remoteIPs))
	txnPartitions.replyChans = make([]chan msgReply, len(tm.remoteIPs))
	txnPartitions.prepInfoPerPartition = make([]tools.Pair[int32, clocksi.Timestamp], nGoRoutines)

	//First request may be special, as it may be a server connection/server replicaID request.
	//If it is a server connection, handleFirstRequest will call the appropriate handler that will keep listening for requests until the connection is closed.
	stop, updBuf, readBuf := tm.handleFirstRequest(txnPartitions, id, rng, channel)
	if stop { //Server-conn that dropped, or client somehow crashed before we got any proper request.
		close(channel)
		return
	}
	/*
		if firstReq.Args.getRequestType() != serverConnRequest {
			tm.connPool.newConn()
			stop = tm.handleTMRequest(firstReq, txnPartitions)
		}
	*/
	/*
		bufs := TMClientBuffers{
			readChans:        make([]chan crdt.State, 1),
			states:           make([]crdt.State, 1),
			reqsPerServer:    make([][]crdt.ReadObjectParams, len(tm.remoteIPs)),
			remoteReqsToChan: make([][]int, len(tm.remoteIPs)),
		}
		bufs.readChans[0] = make(chan crdt.State, 1)
		for i := 0; i < len(tm.remoteIPs); i++ {
			bufs.reqsPerServer[i] = make([]crdt.ReadObjectParams, 0, 1)
			bufs.remoteReqsToChan[i] = make([]int, 0, 1)
		}
		tm.connPool.newConn()
		stop = tm.handleTMRequest(firstReq, txnPartitions, &bufs, id)
		for !stop {
			request := <-channel
			stop = tm.handleTMRequest(request, txnPartitions, &bufs, id)
		}*/
	//tm.connPool.newConn()
	tm.handleTMRequests(txnPartitions, id, rng, channel, updBuf, readBuf) //Will return on lostConnRequest.
	/*stop = tm.handleTMRequest(firstReq, txnPartitions, rng, id)
	for !stop {
		request := <-channel
		stop = tm.handleTMRequest(request, txnPartitions, rng, id)
	}*/
	close(channel)

	utilities.FancyDebugPrint(utilities.TM_PRINT, tm.replicaID, "connection lost, shutting down goroutine for client.")
}

// Note: This only handles one client. However, it is used for multi-client purposes, as it funnels all replies to a shared reply buffer.
func (tm *TransactionManager) listenForProtobufRequestMultiClient(channel chan TransactionManagerRequest, internalId, clientId int, replyChan chan TMMultiClientReply) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var txnPartitions *ongoingTxn = &ongoingTxn{}
	//txnPartitions.partitions = make([]bool, nGoRoutines)
	txnPartitions.partitions = tools.NewBitSet(int(nGoRoutines))
	txnPartitions.debugID = rng.Intn(10)
	txnPartitions.ongoingRemote = ongoingRemote{}
	txnPartitions.lockChans = make([]chan msgToSend, len(tm.remoteIPs))
	txnPartitions.replyChans = make([]chan msgReply, len(tm.remoteIPs))
	txnPartitions.prepInfoPerPartition = make([]tools.Pair[int32, clocksi.Timestamp], nGoRoutines)

	tm.connPool.newConn()

	tm.handleTMRequestsMultiClient(txnPartitions, internalId, clientId, replyChan, channel, rng) //Will return on lostConnRequest.
	close(channel)

	utilities.FancyDebugPrint(utilities.TM_PRINT, tm.replicaID, "connection lost, shutting down goroutine for client.")
}

func (tm *TransactionManager) handleFirstRequest(txnPartitions *ongoingTxn, id int, rng *rand.Rand,
	channel chan TransactionManagerRequest) (lostConn bool, updBuf *tmUpdBuffers, readBuf *tmReadBuffers) {
	request := <-channel
	//First check if it is a special request and handle it appropriately (server connection or a lost connection)
	switch request.Args.getRequestType() {
	case serverConnRequest:
		typedArgs := request.Args.(TMServerConn)
		if typedArgs.ReqChan != nil { //If upgrading an existing connection to S2S, we will receive her a new channel with a larger buffer.
			channel = typedArgs.ReqChan
		}
		tm.handleServerRequests(channel, typedArgs.ReplyChan, rng, id)
		return true, nil, nil
	case serverReplicaIDRequest:
		typedArgs := request.Args.(TMReplicaID)
		if typedArgs.ReqChan != nil { //If upgrading an existing connection to S2S, we will receive her a new channel with a larger buffer.
			channel = typedArgs.ReqChan
		}
		tm.handleRemoteReplicaID(typedArgs)
		tm.handleServerRequests(channel, typedArgs.ReplyChan, rng, id)
		//request = <-channel //Next request will be TMServerConn
		//tm.handleServerRequests(channel, request.Args.(TMServerConn).ReplyChan, rng, id)
		return true, nil, nil
	case lostConnRequest:
		*txnPartitions = ongoingTxn{}
		tm.clksInUse[id] = clocksi.HighestTs //We assign HighestTs to avoid a race condition with GC known as "torn interface/read", where the interface is not fully nil but the value already is. Highest will always lead to GC ignoring this clk, as intended.
		tm.canReuseIDChan <- id
		return true, nil, nil
	}
	//Client connection.
	updBuf = &tmUpdBuffers{} //We only initialize this in handleStaticTMUpdate, as clients may be read-only
	readBuf = (&tmReadBuffers{}).Init()
	tm.connPool.newConn()
	switch request.Args.getRequestType() {
	case readStaticTMRequest:
		tm.handleTMStaticReadWithReply(request, id, readBuf)
	case updateStaticTMRequest:
		tm.handleTMStaticUpdateWithReply(request, rng, updBuf)
	case readTMRequest:
		tm.handleTMReadWithReply(request, txnPartitions, readBuf)
	case updateTMRequest:
		tm.handleTMUpdateWithReply(request, txnPartitions, updBuf)
	case startTxnTMRequest:
		tm.handleTMStartTxnWithReply(request, txnPartitions, id, rng)
	case commitTMRequest:
		tm.handleTMCommitWithReply(request, txnPartitions, id)
	case abortTMRequest:
		tm.handleTMAbort(request, txnPartitions, id)
	case newTriggerTMRequest:
		tm.handleNewTrigger(request)
	case getTriggersTMRequest:
		tm.handleGetTriggers(request)
	case getCRDTTMRequest:
		tm.handleGetCRDTWithReply(request)
	case initialDataTMRequest:
		tm.handleInitialDataWithReply(request, rng)
	case manualGCTMRequest:
		tm.gc.RequestManualGC(request.Args.(TMManualGCArgs).ReplyChan)
		//default:
		//fmt.Printf("[TM]Received unknown/unexpected request type %d from client %d, on method handleTMRequest.\n", request.Args.getRequestType(), id)
	}
	return false, updBuf, readBuf
}

func (tm *TransactionManager) handleTMRequests(txnPartitions *ongoingTxn, id int, rng *rand.Rand, channel chan TransactionManagerRequest, updBuf *tmUpdBuffers, readBuf *tmReadBuffers) {
	stop := false
	var request TransactionManagerRequest
	//fmt.Printf("[TM][handleTMRequests]Starting to handle requests for client %d.\n", id)
	for !stop {
		request = <-channel
		switch request.Args.getRequestType() {
		case readStaticTMRequest:
			tm.handleTMStaticReadWithReply(request, id, readBuf)
		case updateStaticTMRequest:
			tm.handleTMStaticUpdateWithReply(request, rng, updBuf)
		case readTMRequest:
			tm.handleTMReadWithReply(request, txnPartitions, readBuf)
		case updateTMRequest:
			tm.handleTMUpdateWithReply(request, txnPartitions, updBuf)
		case startTxnTMRequest:
			tm.handleTMStartTxnWithReply(request, txnPartitions, id, rng)
		case commitTMRequest:
			tm.handleTMCommitWithReply(request, txnPartitions, id)
		case abortTMRequest:
			tm.handleTMAbort(request, txnPartitions, id)
		case newTriggerTMRequest:
			tm.handleNewTrigger(request)
		case getTriggersTMRequest:
			tm.handleGetTriggers(request)
		case getCRDTTMRequest:
			tm.handleGetCRDTWithReply(request)
		case initialDataTMRequest:
			tm.handleInitialDataWithReply(request, rng)
		case manualGCTMRequest:
			tm.gc.RequestManualGC(request.Args.(TMManualGCArgs).ReplyChan)
		case lostConnRequest:
			stop = true
			*txnPartitions, *updBuf = ongoingTxn{}, tmUpdBuffers{}
			tm.clksInUse[id] = clocksi.HighestTs //We assign HighestTs to avoid a race condition with GC known as "torn interface/read", where the interface is not fully nil but the value already is. Highest will always lead to GC ignoring this clk, as intended.
			tm.canReuseIDChan <- id
		default:
			fmt.Printf("[TM]Received unknown/unexpected request type %d from client %d, on method handleTMRequest.\n", request.Args.getRequestType(), id)
		}
		//remoteTxnRequest is handled separatelly
	}
}

func (tm *TransactionManager) handleTMRequestsMultiClient(txnPartitions *ongoingTxn,
	id, clientId int, replyChan chan TMMultiClientReply, reqChan chan TransactionManagerRequest, rng *rand.Rand) (shouldStop bool) {
	stop := false
	var request TransactionManagerRequest
	updBuf := &tmUpdBuffers{} //We only initialize this in handleStaticTMUpdate, as clients may be read-only
	readBuf := (&tmReadBuffers{}).Init()

	var result interface{} = nil
	//Trigger-related requests are not supported here (nor intended to be created under multi-client - should be a separate client creating them.)
	for !stop {
		request = <-reqChan
		isUpdReq := false
		switch request.Args.getRequestType() {
		case readStaticTMRequest:
			result = tm.handleStaticTMRead(request, id, readBuf)
		case updateStaticTMRequest:
			result, isUpdReq = tm.handleStaticTMUpdate(request, rng, updBuf), true
		case readTMRequest:
			result = tm.handleTMRead(request, txnPartitions, readBuf)
		case updateTMRequest:
			result, isUpdReq = tm.handleTMUpdate(request, txnPartitions, updBuf), true
		case startTxnTMRequest:
			result = tm.handleTMStartTxn(request, txnPartitions, id, rng)
		case commitTMRequest:
			result = tm.handleTMCommit(request, txnPartitions, id)
		case abortTMRequest:
			tm.handleTMAbort(request, txnPartitions, id)
		case getCRDTTMRequest:
			result = tm.handleGetCRDT(request)
		case lostConnRequest:
			shouldStop = true
			*txnPartitions, *updBuf, *readBuf = ongoingTxn{}, tmUpdBuffers{}, tmReadBuffers{}
			tm.clksInUse[id] = clocksi.HighestTs //We assign HighestTs to avoid a race condition with GC known as "torn interface/read", where the interface is not fully nil but the value already is. Highest will always lead to GC ignoring this clk, as intended.
			tm.canReuseIDChan <- id
		}
		if result != nil {
			replyChan <- TMMultiClientReply{ClientID: clientId, TxnId: request.TransactionId, Reply: result}
		}
		if isUpdReq { //Idea: we clean the buffers after replying to the client, thus allowing PotionDB to reply to the client in the meantime.
			updBuf.reset()
		}
	}

	return
}

func (tm *TransactionManager) handleRemoteMsgs() {
	/*lastTs, currTs, minDiff := int64(0), int64(0), int64(time.Millisecond)*500
	ignore(lastTs, currTs, minDiff)*/
	nTxnsSinceClean := 0
	remoteWg := sync.WaitGroup{}
	nGroupTxns := 0
	for {
		//currTs = time.Now().UnixNano()
		request := <-tm.remoteChan
		//fmt.Println("[TM]Got request from Replicator")
		switch typedReq := request.(type) {
		/*case TMRemoteClk:
			if tm.inDownQueue > 0 || nGroupTxns == 0 {
				tm.applyRemoteClk(&typedReq, &remoteWg)
			} else if len(tm.remoteChan) == 0 {
				tm.remoteClock.UpdatePos(clocksi.GetSortedPosOfId(typedReq.ReplicaID), typedReq.StableTs)
				tm.applyRemoteTxnGroup(&remoteWg)
			} else {
				tm.remoteClock.UpdatePos(clocksi.GetSortedPosOfId(typedReq.ReplicaID), typedReq.StableTs)
			}
		case RemoteTxn:
			if tm.inDownQueue > 0 {
				tm.applyRemoteTxn(&typedReq, &remoteWg)
			} else {
				tm.remoteTxnHelper(typedReq, &remoteWg, &nGroupTxns, len(tm.remoteChan))
			}
			nTxnsSinceClean++
			tm.remoteTxnsProc++*/

		case TMRemoteClk:
			fmt.Printf("[TM]Applying stable clk from replicaID %d with stableTs %d at %s. Remote clock before applying: %s.\n", typedReq.ReplicaID, typedReq.StableTs, time.Now().Format("15:04:05.000"), tm.remoteClock.ToString())
			tm.applyRemoteClk(&typedReq, &remoteWg)
			//fmt.Printf("[TM]Ignored stable clk from replicaID %d with stableTs %d at %s. Remote clock: %s.\n", typedReq.ReplicaID, typedReq.StableTs, time.Now().Format("15:04:05.000"), tm.remoteClock.ToString())
		case RemoteTxn:
			sortedSenderID := clocksi.GetSortedPosOfId(typedReq.SenderID)
			if typedReq.Clk.GetPos(sortedSenderID) < tm.remoteClock.GetPos(sortedSenderID) {
				panic(fmt.Sprintf("Received remote txn has the senderID's clk value behind tm.remoteClock! SenderID: %d. Our ID: %d. Received clk: %s. RemoteClk: %s. Comparing clocks (rec vs remote): %s. Time: %s\n",
					typedReq.SenderID, tm.replicaID, typedReq.Clk.ToString(), tm.remoteClock.ToString(), typedReq.Clk.ToDebugCompString(tm.remoteClock), time.Now().Format("15:04:05.000")))
			}
			tm.applyRemoteTxn(&typedReq, &remoteWg)
			nTxnsSinceClean++
			tm.remoteTxnsProc++
			//fmt.Printf("[TM]Ignored remote txn from senderID %d with clk %s at %s. Remote clock: %s.\n", typedReq.SenderID, typedReq.Clk.ToString(), time.Now().Format("15:04:05.000"), tm.remoteClock.ToString())
			/*case RemoteTxnGroup:
			tm.applyRemoteTxnGroup(&typedReq)
			nTxnsSinceClean++*/
		/*if currTs-lastTs > minDiff {
			fmt.Printf("[TM]Finished RemoteTxnGroup at %s.\n", time.Unix(0, currTs).Format("2006-01-02 15:04:05.000"))
			lastTs = currTs
		}*/
		case TMGetSnapshot:
			tm.handleTMGetSnapshot(&typedReq)
			if len(tm.remoteChan) == 0 && nGroupTxns > 0 {
				tm.applyRemoteTxnGroup(&remoteWg)
			}
		case TMApplySnapshot:
			tm.handleTMApplySnapshot(&typedReq)
			if len(tm.remoteChan) == 0 && nGroupTxns > 0 {
				tm.applyRemoteTxnGroup(&remoteWg)
			}
		case TMReplicaID:
			tm.handleReplicaID(&typedReq)
			if len(tm.remoteChan) == 0 && nGroupTxns > 0 {
				tm.applyRemoteTxnGroup(&remoteWg)
			}
		case TMRemoteTrigger:
			tm.handleRemoteTrigger(&typedReq)
			if len(tm.remoteChan) == 0 && nGroupTxns > 0 {
				tm.applyRemoteTxnGroup(&remoteWg)
			}
		case TMStart:
			tm.handleTMStart(&typedReq)
			if len(tm.remoteChan) == 0 && nGroupTxns > 0 {
				tm.applyRemoteTxnGroup(&remoteWg)
			}
		}
		if nTxnsSinceClean > 0 && len(tm.remoteChan) == 0 { //We opportunistically clean buffers.
			nTxnsSinceClean = 0
			for i := range tm.bufPendingRemoteTxns {
				tm.bufPendingRemoteTxns[i].DeepClear()
			}
		}
		if tm.remoteTxnsProc&511 == 0 {
			fmt.Printf("[TM]Processed %d remote txns so far at %s, nQueued: %d.\n", tm.remoteTxnsProc, time.Now().Format("15:04:05.000"), tm.nEverQueued)
		}
		//fmt.Println("[TM]Finished request from Replicator")
	}
}

func (tm *TransactionManager) handleRemoteReplicaID(req TMReplicaID) {
	remoteID := req.ReplicaID
	tm.RemoteInfo.Lock()
	if _, has := tm.RemoteInfo.remoteIDToIndex[remoteID]; !has {
		fmt.Printf("[TM]Adding replicaID %d via S2S at %s\n", remoteID, time.Now().Format("15:04:05.000"))
		clocksi.AddNewID(remoteID)
		tm.RemoteInfo.remoteBks = append(tm.RemoteInfo.remoteBks, req.Buckets)
		tm.RemoteInfo.remoteIPs = append(tm.RemoteInfo.remoteIPs, req.IP)
		tm.RemoteInfo.remoteIDToIndex[remoteID] = len(tm.RemoteInfo.remoteIPs) - 1
		if len(othersIPList) == len(tm.RemoteInfo.remoteIPs) { //All replicaIDs are known.
			tm.RemoteInfo.Unlock()
			fmt.Printf("[TM]Finishing TM initialization via S2S at %s.\n", time.Now().Format("15:04:05.000"))
			tm.finishTMInitialialization()
			if shared.IsReplDisabled { //If replication is disabled, we only needed to wait for S2S, so we can reply as being fully ready.
				tm.waitStartChan <- BOTH_READY
			} else {
				tm.waitStartChan <- TM_READY
			}
		} else {
			tm.RemoteInfo.Unlock()
		}
	} else {
		//else: ignore.
		tm.RemoteInfo.Unlock()
		fmt.Printf("[TM]Ignored replicaID %d via S2S at %s as we already know that replicaID.\n", remoteID, time.Now().Format("15:04:05.000"))
	}
}

func (tm *TransactionManager) handleServerRequests(channel chan TransactionManagerRequest, replyChan chan TMS2SReply, rng *rand.Rand, id int) {
	ongoingInfo := make(map[uint64]*ongoingTxn) //We sadly can't use a slice (could use tools.SliceMap though), as clientIDs are sparse and are managed by other server's CPs.
	/*staticReadReplyChan, staticUpdateReplyChan := make(chan TMStaticReadReply, 1), make(chan TMStaticUpdateReply, 1)
	readReplyChan, updateReplyChan := make(chan []crdt.State, 1), make(chan TMUpdateReply, 1)
	commitChan, startTxnChan := make(chan TMCommitReply, 1), make(chan TMStartTxnReply, 1)*/
	updBuf := &tmUpdBuffers{}            //Safe as we block for each request until complete.
	readBuf := (&tmReadBuffers{}).Init() //Safe as we block for each request until complete.
	var clientID uint64
	var txnID TransactionId
	var reply TMS2SReply

	for {
		request := <-channel
		innerArgs, ok := request.Args.(TMS2SRequest)
		if !ok {
			if request.Args.getRequestType() == lostConnRequest {
				//Since in ProtoServer we close the connection to the other server, here we will stop too.
				//fmt.Printf("[TM]S2S connection lost with the other server.")
				return
			}
			fmt.Printf("[TM][S2S]Received non-S2S request with type %d. Forcing a system crash as this likely means a bug.\n", request.Args.getRequestType())
			panic(0)
		}

		clientID, txnID = innerArgs.ClientID, request.TransactionId
		reply.ClientID, reply.TxnID = clientID, txnID
		//Switch into correct type of request, call the respective method and immediately forward the reply to replyChan.
		switch typedArgs := innerArgs.Args.(type) {
		case TMStaticReadArgs:
			reply.ReplyType = proto.WrapperType_STATIC_READ
			reply.Reply = tm.handleStaticTMRead(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: typedArgs}, id, readBuf)
		case TMSingleReadArgs:
			reply.ReplyType = proto.WrapperType_STATIC_SINGLE_READ
			reply.Reply = tm.handleSingleRead(id, typedArgs.ReadParams, readBuf)
		case TMStaticUpdateArgs:
			reply.ReplyType = proto.WrapperType_STATIC_UPDATE
			reply.Reply = tm.handleStaticTMUpdate(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: typedArgs}, rng, updBuf)
		case TMReadArgs:
			reply.ReplyType = proto.WrapperType_READ
			reply.Reply = tm.handleTMRead(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: typedArgs}, ongoingInfo[clientID], readBuf)
		case TMUpdateArgs:
			reply.ReplyType = proto.WrapperType_UPD
			reply.Reply = tm.handleTMUpdate(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: typedArgs}, ongoingInfo[clientID], updBuf)
		case TMStartTxnArgs:
			reply.ReplyType = proto.WrapperType_START_TXN
			reply.Reply = tm.handleTMStartTxn(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: typedArgs}, ongoingInfo[clientID], id, rng)
		case TMCommitArgs:
			reply.ReplyType = proto.WrapperType_COMMIT
			reply.Reply = tm.handleTMCommit(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: typedArgs}, ongoingInfo[clientID], id)
		case TMAbortArgs:
			reply.ReplyType = proto.WrapperType_ABORT
			tm.handleTMAbort(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: typedArgs}, ongoingInfo[clientID], id)
			reply.Reply = TMCommitReply{Timestamp: request.Timestamp}
		case TMBCPermsArgs: //No reply.
			tm.handleTMBCPerms(typedArgs)
		case TMInitialDataArgs: //It is not expected for S2S to be used for this purpose.
			reply.ReplyType = proto.WrapperType_STATIC_UPDATE
			tm.handleInitialDataWithReply(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: typedArgs}, rng)
			reply.Reply = TMStaticUpdateReply{TransactionId: txnID, Timestamp: request.Timestamp}
		default:
			fmt.Println("[TM][S2S]Unknown request type for S2S:", innerArgs.Args.getRequestType())
			panic(fmt.Sprintf("[TM][S2S]Unknown request type for S2S: %d", innerArgs.Args.getRequestType()))
		}
		if reply.Reply != nil { //Some requests don't need reply, such as BC_PERMS_ARGS.
			replyChan <- reply
		}
		reply.Reply = nil
	}
}

// A single routine of handleServerRequests may receive requests from multiple clients.
// We can't block, as this may lead to everyone blocking when trying to make distributed commits.
// Thus, we must execute non-blocking versions of TM's operations, and forward the reply to another goroutine.
// Alternative is starting a new goroutine per request, but this is too heavy. A goroutine per clientID is also heavy as each client may require connections to all servers.
// A decoupled processing + reply (i.e., one routine for each) works fine, as clients will block on remote requests anyway.
// An alternative is, remote commits can be non-blocking, by replying as soon as the commit is either applied or queued to be applied.
// Now the only thing that worries me is blocking reads (which we don't really support atm)
func (tm *TransactionManager) handleServerRequestsOld(channel chan TransactionManagerRequest, replyChan chan TMS2SReply, rng *rand.Rand, id int) {
	stop := false
	ongoingInfo := make(map[uint64]*ongoingTxn) //TODO: This could be a slice if we ensure clientIDs are incremental?
	//TODO: Undo, as this doesn't seem to be the problem.
	//updBuf := &tmUpdBuffers{} //One buffer is enough, as for each request we wait for the reply before processing the next request.
	//readBuf := (&tmReadBuffers{}).Init()
	/*bufs := TMClientBuffers{
		readChans:        make([]chan crdt.State, 1),
		states:           make([]crdt.State, 1),
		reqsPerServer:    make([][]crdt.ReadObjectParams, len(tm.remoteIPs)),
		remoteReqsToChan: make([][]int, len(tm.remoteIPs)),
	}
	bufs.readChans[0] = make(chan crdt.State, 1)
	for i := 0; i < len(tm.remoteIPs); i++ {
		bufs.reqsPerServer[i] = make([]crdt.ReadObjectParams, 0, 1)
		bufs.remoteReqsToChan[i] = make([]int, 0, 1)
	}*/

	idsOK := tools.NewSliceWithCounter[uint64](1000) //TODO: DELETE.
	for !stop {
		if idsOK.Len() == 1000 {
			var sb strings.Builder
			sb.WriteString("[TM][S2S]Last 1000 IDs processed: [")
			for i := 0; i < idsOK.Len(); i++ {
				sb.WriteString(fmt.Sprintf("%d, ", idsOK.Get(i)))
			}
			sb.WriteString("]")
			fmt.Println(sb.String())
			idsOK.DeepClear()
		}
		//TODO: UNDO?
		updBuf := &tmUpdBuffers{} //One buffer is enough, as for each request we wait for the reply before processing the next request.
		readBuf := (&tmReadBuffers{}).Init()
		//fmt.Println("[TM]Waiting for S2S request")
		request := <-channel
		innerArgs, ok := request.Args.(TMS2SRequest)
		if !ok {
			if request.Args.getRequestType() == lostConnRequest {
				//fmt.Printf("[TM]Connection lost for client. Closing S2S connection with server ID %d\n", id)
				//stop = true
				//break
				fmt.Printf("[TM]Connection lost for client. Will keep S2S connection with server ID %d alive.\n", id)
				continue
			}
			panic(0)
		}
		//fmt.Println("[TM]Got S2S request:", innerArgs.Args.getRequestType())
		clientID, txnID := innerArgs.ClientID, request.TransactionId
		tmpChan := make(chan struct{}, 1)
		//So... no one getting stuck here...?
		//TODO: UNDO THIS CHAN.
		//fmt.Printf("[TM][S2S]Started request with txnID %d, clientID %d.\n", txnID, clientID)
		go func(tChan chan struct{}, tID TransactionId, cID uint64) {
			select {
			case <-tChan:
				//OK, nothing to do.
			case <-time.After(5 * time.Second):
				fmt.Printf("[TM][S2S]Timeout!!! For request type %d, request clk %s, TM id %d, client %d, txnID %d. Op args: (%T) %+v. Exitting.\n",
					innerArgs.Args.getRequestType(), request.Timestamp.ToString(), id, cID, tID, innerArgs.Args, innerArgs.Args)
				time.Sleep(time.Duration(500+rng.Intn(500)) * time.Millisecond)
				os.Exit(1)
			}
		}(tmpChan, txnID, clientID)
		switch innerArgs.Args.getRequestType() {
		case readStaticTMRequest:
			/*go func(clientID int32, txnID TransactionId, channel chan TMStaticReadReply) {
				replyChan <- TMS2SReply{ClientID: clientID, TxnID: txnID, ReplyType: proto.WrapperType_STATIC_READ_OBJS, Reply: <-channel}
			}(clientID, txnID, innerArgs.Args.(TMStaticReadArgs).ReplyChan)*/
			tm.handleTMStaticReadWithReply(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: innerArgs.Args}, id, readBuf)
		case readSingleTMRequest:
			singleReadArgs := innerArgs.Args.(TMSingleReadArgs)
			tm.handleSingleReadWithReply(id, singleReadArgs.ReadParams, singleReadArgs.ReplyChan, readBuf)

		case updateStaticTMRequest:
			/*go func(clientID int32, txnID TransactionId, channel chan TMStaticUpdateReply) {
				replyChan <- TMS2SReply{ClientID: clientID, TxnID: txnID, ReplyType: proto.WrapperType_COMMIT, Reply: <-channel}
			}(clientID, txnID, innerArgs.Args.(TMStaticUpdateArgs).ReplyChan)*/
			//fmt.Printf("[TM][S2S]Static update from server %d, txn %d.", innerArgs.ClientID, txnID)
			/*var sb strings.Builder
			upds := innerArgs.Args.(TMStaticUpdateArgs).UpdateParams
			for _, upd := range upds {
				sb.WriteString(fmt.Sprintf("(%s, %s, %s), ", upd.Key, upd.CrdtType, upd.Bucket))
			}*/
			/*copyUpds := make([]crdt.UpdateObjectParams, len(upds))
			for i, upd := range upds {
				copyUpds[i] = crdt.UpdateObjectParams{
					KeyParams:  crdt.KeyParams{Key: strings.Clone(upd.Key), CrdtType: upd.CrdtType, Bucket: strings.Clone(upd.Bucket)},
					UpdateArgs: upd.UpdateArgs}
			}
			copyArgs := TMStaticUpdateArgs{UpdateParams: copyUpds, ReplyChan: innerArgs.Args.(TMStaticUpdateArgs).ReplyChan}*/
			//fmt.Printf("[TM][S2S]Static update from server %d, txn %d, upds keys: %s\n", innerArgs.ClientID, txnID, sb.String())
			tm.handleTMStaticUpdateWithReply(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: innerArgs.Args}, rng, updBuf)
		case readTMRequest:
			/*go func(clientID int32, txnID TransactionId, channel chan []crdt.State) {
				replyChan <- TMS2SReply{ClientID: clientID, TxnID: txnID, ReplyType: proto.WrapperType_READ_OBJS, Reply: <-channel}
			}(clientID, txnID, innerArgs.Args.(TMReadArgs).ReplyChan)*/
			tm.handleTMReadWithReply(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: innerArgs.Args}, ongoingInfo[clientID], readBuf)
		case updateTMRequest:
			/*go func(clientID int32, txnID TransactionId, channel chan TMUpdateReply) {
				replyChan <- TMS2SReply{ClientID: clientID, TxnID: txnID, ReplyType: proto.WrapperType_UPD, Reply: <-channel}
			}(clientID, txnID, innerArgs.Args.(TMUpdateArgs).ReplyChan)*/
			tm.handleTMUpdateWithReply(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: innerArgs.Args}, ongoingInfo[clientID], updBuf)
		case startTxnTMRequest:
			txnPartitions, has := ongoingInfo[clientID]
			if !has {
				txnPartitions = &ongoingTxn{}
				ongoingInfo[clientID] = txnPartitions
				txnPartitions.partitions, txnPartitions.debugID, txnPartitions.ongoingRemote = tools.NewBitSet(int(nGoRoutines)), rng.Intn(10), ongoingRemote{}
				txnPartitions.lockChans, txnPartitions.replyChans = make([]chan msgToSend, len(tm.remoteIPs)), make([]chan msgReply, len(tm.remoteIPs))
				txnPartitions.prepInfoPerPartition = make([]tools.Pair[int32, clocksi.Timestamp], nGoRoutines)
			} else {
				txnPartitions.reset()
			}
			/*go func(clientID int32, txnID TransactionId, channel chan TMStartTxnReply) {
				replyChan <- TMS2SReply{ClientID: clientID, TxnID: txnID, ReplyType: proto.WrapperType_START_TXN, Reply: <-channel}
			}(clientID, txnID, innerArgs.Args.(TMStartTxnArgs).ReplyChan)*/
			tm.handleTMStartTxnWithReply(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: innerArgs.Args}, txnPartitions, id, rng)
		case commitTMRequest:
			/*go func(clientID int32, txnID TransactionId, channel chan TMCommitReply) {
				replyChan <- TMS2SReply{ClientID: clientID, TxnID: txnID, ReplyType: proto.WrapperType_COMMIT, Reply: <-channel}
			}(clientID, txnID, innerArgs.Args.(TMCommitArgs).ReplyChan)*/
			tm.handleTMCommitWithReply(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: innerArgs.Args}, ongoingInfo[clientID], id)
		case abortTMRequest:
			tm.handleTMAbort(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: innerArgs.Args}, ongoingInfo[clientID], id)
			//Doesn't need reply
		case bcPermsTMRequest:
			tm.handleTMBCPerms(innerArgs.Args.(TMBCPermsArgs))
		case initialDataTMRequest:
			tm.handleInitialDataWithReply(TransactionManagerRequest{TransactionId: txnID,
				Timestamp: request.Timestamp, Args: innerArgs.Args}, rng)
		default:
			fmt.Println("[TM]Unknown request type for S2S:", innerArgs.Args.getRequestType())
			panic(fmt.Sprintf("[TM]Unknown request type for S2S: %d", innerArgs.Args.getRequestType()))
		}
		tmpChan <- struct{}{}
		idsOK.AddToEnd(uint64(clientID))
		//fmt.Printf("[TM][S2S]Finished request with txnID %d, clientID %d.\n", txnID, clientID)
	}
}

type ProcessReadsBuffer struct {
	readChans                       []chan crdt.State
	states                          []crdt.State
	nRepliesReceived                int //Unused
	crdt.ReadProcessingObjectParams     //Original read
}

func (tm *TransactionManager) handleTMStaticReadWithReply(request TransactionManagerRequest, id int, readBuf *tmReadBuffers) {
	request.Args.(TMStaticReadArgs).ReplyChan <- tm.handleStaticTMRead(request, id, readBuf)
}

func (tm *TransactionManager) handleSingleReadWithReply(id int, readArgs crdt.ReadObjectParams, replyChan chan TMStaticReadReply, readBuf *tmReadBuffers) {
	replyChan <- tm.handleSingleRead(id, readArgs, readBuf)
}

func (tm *TransactionManager) handleSingleRead(id int, readArgs crdt.ReadObjectParams, readBuf *tmReadBuffers) (result TMStaticReadReply) {
	//readChan := make(chan StateClockPair, 1)
	isRemote, serverIndex := tm.getReadLocation(readArgs.Bucket)

	if !isRemote {
		tm.mat.SendRequest(MaterializerRequest{MatRequestArgs: MatStaticSingleReadArgs{
			ReadObjectParams: readArgs, ReplyChan: readBuf.singleReadChan}})
	} else {
		go tm.handleRemoteStaticSingleRead(id, readArgs, serverIndex, readBuf.singleReadChan)
	}

	reply := <-readBuf.singleReadChan
	readBuf.singleReadStates[0] = reply.State
	return TMStaticReadReply{States: readBuf.singleReadStates, Timestamp: reply.Timestamp}
	//close(readChan)
	//return TMStaticReadReply{States: []crdt.State{reply.State}, Timestamp: reply.Timestamp}
}

// func (tm *TransactionManager) handleStaticTMRead(request TransactionManagerRequest, bufs *TMClientBuffers, id int) {
func (tm *TransactionManager) handleStaticTMRead(request TransactionManagerRequest, id int, readBuf *tmReadBuffers) (reply TMStaticReadReply) {
	readArgs := request.Args.(TMStaticReadArgs)
	/*if len(readArgs.ReadParams) == 2 {
		fmt.Printf("[TM]Got static full read with 2 reads, first read: %+v.\n Second read: %+v.\n", readArgs.ReadParams[0], readArgs.ReadParams[1])
	} else {
		fmt.Printf("[TM]Got static full read with %d reads, first read: %+v.\n", len(readArgs.ReadParams), readArgs.ReadParams[0])
	}*/
	if FAST_SINGLE_READ && len(readArgs.ReadParams) == 1 {
		return tm.handleSingleRead(id, readArgs.ReadParams[0], readBuf)
	}
	//tsToUse := request.Timestamp
	tsToUse := tm.getClockToUse(request.Timestamp, id)
	//tsToUse := tm.localClock.Copy()

	var historyPos int
	if doCompactHistory {
		historyPos = tm.clocksArray.Write(tsToUse)
	}

	var currRequest MaterializerRequest
	//readChans := make([]chan crdt.State, len(readArgs.ReadParams))
	if len(readArgs.ReadParams) > cap(readBuf.states) {
		readBuf.readChan, readBuf.states = make(chan tools.Pair[int, crdt.State], len(readArgs.ReadParams)), make([]crdt.State, len(readArgs.ReadParams))
	}
	readChan, states := readBuf.readChan, readBuf.states[:len(readArgs.ReadParams)]
	//readChan := make(chan tools.Pair[int, crdt.State], len(readArgs.ReadParams))
	//states := make([]crdt.State, len(readArgs.ReadParams))
	//processReads := make([]ProcessReadsBuffer, 0, 1)

	//reqsPerServer := make([][]crdt.ReadObjectParams, len(tm.remoteIPs))
	//remoteReqsToChan := make([][]int, len(tm.remoteIPs))
	var reqsPerServer [][]crdt.ReadObjectParams
	var remoteReqsToChan [][]int
	/*if len(readArgs.ReadParams) > len(bufs.readChans) {
		bufs.readChans = make([]chan crdt.State, len(readArgs.ReadParams))
		bufs.states = make([]crdt.State, len(readArgs.ReadParams))
		for i := 0; i < len(readArgs.ReadParams); i++ {
			bufs.readChans[i] = make(chan crdt.State, 1)
		}
	}*/
	isRemote, serverIndex, hasRemote := true, 0, false

	//tsStart := time.Now().UnixNano()
	for i, currRead := range readArgs.ReadParams {
		/*if currRead.ReadArgs.GetREADType() == proto.READType_PROCESS {
			procParams := currRead.ReadArgs.(crdt.ReadProcessingObjectParams)
			procRead := tm.processStaticReadHelper(procParams.PreReads, tsToUse)
			procRead.ReadProcessingObjectParams = procParams
			processReads = append(processReads, procRead)
		}*/
		//readChans[i] = make(chan crdt.State, 1)
		isRemote, serverIndex = tm.getReadLocation(currRead.Bucket)

		if !isRemote {
			//fmt.Printf("[TM]Read args: %+v (%T), %v, %v. Bucket: %v\n",
			//currRead.ReadArgs, currRead.ReadArgs, currRead.ReadArgs.GetCRDTType(), currRead.ReadArgs.GetREADType(), currRead.KeyParams)
			currRequest = MaterializerRequest{
				MatRequestArgs: MatStaticReadArgs{MatReadCommonArgs: MatReadCommonArgs{
					Timestamp:        tsToUse,
					ReadObjectParams: currRead,
					ReplyChan:        readChan,
					ReplyIndex:       i,
					//ReplyChan:        readChans[i],
					//ReplyChan:        bufs.readChans[i],
					//HashKey: new(uint64),
				}},
			}
			tm.mat.SendRequest(currRequest)
		} else {
			/*bufs.reqsPerServer[serverIndex] = append(bufs.reqsPerServer[serverIndex], currRead)
			bufs.remoteReqsToChan[serverIndex] = append(bufs.remoteReqsToChan[serverIndex], i)*/
			if !hasRemote { //First time we see a remote read, initialize the slices. Most often, there won't be reads to other replicas, thus the conservative approach.
				reqsPerServer, remoteReqsToChan = make([][]crdt.ReadObjectParams, len(tm.remoteIPs)), make([][]int, len(tm.remoteIPs))
			}
			reqsPerServer[serverIndex] = append(reqsPerServer[serverIndex], currRead)
			remoteReqsToChan[serverIndex] = append(remoteReqsToChan[serverIndex], i)
			hasRemote = true
		}
	}

	if hasRemote {
		go tm.handleRemoteStaticReads(id, tsToUse, reqsPerServer, remoteReqsToChan, readChan)
		//go tm.handleRemoteStaticReads(request.TransactionId, tsToUse, bufs)
	}

	//for i, readChan := range bufs.readChans {
	//bufs.states[i] = <-readChan
	/*for i, processRead := range processReads {
		for j, readChan := range processRead.readChans {
			states[j] = <-readChan
			close(readChan)
		}
		*processRead.Result = tm.aggregateStates(states, processRead.AggregateType)
		processReads[i] = tm.processStaticReadHelper(processRead.PostReads, tsToUse)
	}*/

	/*for i, readChan := range readChans {
		states[i] = <-readChan
		//fmt.Printf("[TM]Read %+v, State %+v\n", readArgs.ReadParams[i], states[i])
		close(readChan)
	}*/
	var curr tools.Pair[int, crdt.State]
	for i := 0; i < len(readArgs.ReadParams); i++ {
		curr = <-readChan
		states[curr.First] = curr.Second
	}
	//close(readChan)
	//tsEnd := time.Now().UnixNano()
	//fmt.Printf("[TM]Static read took %d microseconds.\n", (tsEnd-tsStart)/int64(time.Duration(time.Microsecond)))

	//Collecting results of postReads
	/*for _, processRead := range processReads {
		stateHolder := crdt.ProcessState{States: make([]crdt.State, len(processRead.PostReads))}
		for j, readChan := range processRead.readChans {
			stateHolder.States[j] = <-readChan
			close(readChan)
		}
		//TODO: Where to put the result?
	}*/

	tm.clksInUse[id] = clocksi.HighestTs //We assign HighestTs to avoid a race condition with GC known as "torn interface/read", where the interface is not fully nil but the value already is. Highest will always lead to GC ignoring this clk, as intended.
	if doCompactHistory {
		tm.clocksArray.Delete(historyPos)
	}

	/*for i, state := range states {
		fmt.Printf("[TM][StaticRead]State %d (%T): %+v\n", i, state, state)
	}*/

	//fmt.Println("[TM]Static read with clk: ", tsToUse)
	return TMStaticReadReply{States: states, Timestamp: tsToUse}
}

func (tm *TransactionManager) aggregateStates(states []crdt.State, aggregType crdt.AggregateType) (result float64) {
	switch aggregType {
	case crdt.M_MAX:
		result = math.Inf(-1)
		for _, state := range states {
			switch typedState := state.(type) {
			case crdt.CounterState:
				if float64(typedState) > result {
					result = float64(typedState)
				}
			case crdt.CounterFloatState:
				if float64(typedState) > result {
					result = float64(typedState)
				}
			case crdt.AvgFullState:
				avg := float64(typedState.Sum) / float64(typedState.NAdds)
				if avg > result {
					result = avg
				}
			case crdt.AvgState:
				if typedState.Value > result {
					result = typedState.Value
				}
			case crdt.RegisterState:
				value := typedState.ToFloat64()
				if value > result {
					result = value
				}
			}
		}
	case crdt.M_MIN:
		result = math.Inf(1)
		for _, state := range states {
			switch typedState := state.(type) {
			case crdt.CounterState:
				if float64(typedState) < result {
					result = float64(typedState)
				}
			case crdt.CounterFloatState:
				if float64(typedState) < result {
					result = float64(typedState)
				}
			case crdt.AvgFullState:
				avg := float64(typedState.Sum) / float64(typedState.NAdds)
				if avg < result {
					result = avg
				}
			case crdt.AvgState:
				if typedState.Value < result {
					result = typedState.Value
				}
			case crdt.RegisterState:
				value := typedState.ToFloat64()
				if value < result {
					result = value
				}
			}
		}
	case crdt.M_AVG:
		count := 0.0
		for _, state := range states {
			switch typedState := state.(type) {
			case crdt.CounterState:
				result += float64(typedState)
				count++
			case crdt.CounterFloatState:
				result += float64(typedState)
				count++
			case crdt.AvgFullState:
				result += float64(typedState.Sum)
				count += float64(typedState.NAdds)
			case crdt.AvgState:
				result += typedState.Value
				count++
			case crdt.RegisterState:
				result += typedState.ToFloat64()
				count++
			}
		}
	case crdt.M_SUM:
		for _, state := range states {
			switch typedState := state.(type) {
			case crdt.CounterState:
				result += float64(typedState)
			case crdt.CounterFloatState:
				result += float64(typedState)
			case crdt.AvgFullState:
				result += float64(typedState.Sum) / float64(typedState.NAdds)
			case crdt.AvgState:
				result += typedState.Value
			case crdt.RegisterState:
				result += typedState.ToFloat64()
			}
		}
	}
	return result
}

// Sends the preReads in a ReadProcessingObjectParams to the materializer's partitions
/*func (tm *TransactionManager) processStaticReadHelper(reads []crdt.ReadObjectParams, tsToUse clocksi.Timestamp) (buf ProcessReadsBuffer) {
	buf.states, buf.readChans = make([]crdt.State, len(reads)), make([]chan crdt.State, len(reads))
	buf.nRepliesReceived = 0
	var currRequest MaterializerRequest
	for j, read := range reads {
		buf.readChans[j] = make(chan crdt.State, 1)
		currRequest = MaterializerRequest{
			MatRequestArgs: MatStaticReadArgs{MatReadCommonArgs: MatReadCommonArgs{
				Timestamp:        tsToUse,
				ReadObjectParams: read,
				ReplyChan:        buf.readChans[j],
			}},
		}
		tm.mat.SendRequest(currRequest)
	}
	return buf
}*/

func (tm *TransactionManager) handleInitialDataWithReply(request TransactionManagerRequest, rng *rand.Rand) {
	tm.handleInitialData(request, rng)
	//fmt.Printf("[TM][InitData]Replying to client informing that initial data request is complete.\n")
	request.Args.(TMInitialDataArgs).ReplyChan <- true
}

// This request will wait until Materializer finishes applying the commit. This is useful for initialization and GC purposes.
// This will however NOT wait for updates forwarded to remote replicas. While those *should* work, it is not intended for this method to forward requests.
// TM's clock is still updated normally, to ensure PotionDB's GC works properly
// IMPORTANT NOTE: This is really for initialization of data. There is no coordination between partitions. As such, concurrent initializations to the same CRDT may break causality.
func (tm *TransactionManager) handleInitialData(request TransactionManagerRequest, rng *rand.Rand) {
	//fmt.Printf("[TM][InitData]Received request to insert initial data.\n")
	initArgs := request.Args.(TMInitialDataArgs)

	//fmt.Printf("[TM][InitialData]Received request to insert initial data with %d updates.\n", len(initArgs.UpdateParams))
	//No need to use buffers, as handleInitialData will only be used for initialization.
	updsPerPartition, reqsPerServer, hasRemote := tm.groupWritesNoBuf(initArgs.UpdateParams)
	//replyChan := make(chan int, len(updsPerPartition))
	waitFor := 0

	/*var copyData []int64 = make([]int64, len(tm.replicaIDs))
	tm.localClock.Lock()
	tm.localClock.FastCopyInto(copyData)
	tm.localClock.Unlock()
	txnClk := clocksi.FromSliceValuesToSliceTimestamp(copyData)*/
	txnClk := tm.localClock.GetClock().NextTimestamp(shared.SortedReplicaID)
	fakeClk := clocksi.NewSliceTimestamp() //We use an all 0 timestamp for initial data in partitions.
	txnId := TransactionId(rng.Uint64())

	reqs := make([]MaterializerRequest, len(updsPerPartition))
	var wg sync.WaitGroup
	for _, partUpdates := range updsPerPartition {
		if partUpdates != nil {
			reqs[waitFor] = MaterializerRequest{
				MatRequestArgs: MatInitialDataArgs{
					TransactionId: txnId,
					Updates:       partUpdates,
					//ReplyChan:     replyChan,
					Wg:        &wg,
					Timestamp: fakeClk,
				},
			}
			waitFor++
		}
	}
	tm.commitChan <- TMCommitNPartitions{nPartitions: waitFor, txnId: txnId, clk: txnClk}
	//tm.commitChan.Write(TMCommitNPartitions{nPartitions: waitFor, txnId: txnId, clk: txnClk})

	//No coordination. We just send the request directly.
	wg.Add(waitFor)
	for i := 0; i < waitFor; i++ {
		//fmt.Printf("[TM][InitialData]Sending initial data to partition %d\n", reqs[i].getChannel())
		tm.mat.SendRequest(reqs[i])
	}

	if hasRemote {
		tm.handleRemoteStaticUpds(request.TransactionId, clocksi.DummyTs, reqsPerServer)
	}

	//Wait for each partition to finish applying the updates.
	//fmt.Printf("[TM][InitialData]Waiting for %d partitions to finish applying initial data.\n", waitFor)
	/*for i := 0; i < waitFor; i++ {
		<-replyChan
	}*/
	wg.Wait()
	//fmt.Printf("[TM][InitialData]Finished applying initial data of %d updates.\n", len(initArgs.UpdateParams))
}

func (tm *TransactionManager) handleTMStaticUpdateWithReply(request TransactionManagerRequest, rng *rand.Rand, updBuf *tmUpdBuffers) {
	request.Args.(TMStaticUpdateArgs).ReplyChan <- tm.handleStaticTMUpdate(request, rng, updBuf)
	updBuf.reset() //We reset the buffer after replying to the client, thus allowing PotionDB to reply to the client in the meantime.
}

// TODO: Separate in parts?
func (tm *TransactionManager) handleStaticTMUpdate(request TransactionManagerRequest, rng *rand.Rand, updBuf *tmUpdBuffers) (reply TMStaticUpdateReply) {
	//return TMStaticUpdateReply{TransactionId: 0, Timestamp: nil, Err: nil}
	/*debugChan := make(chan struct{}, 1)
	go func(dc chan struct{}) {
		select {
		case <-dc:
			//Nothing to do.
		case <-time.After(120 * time.Second):
			panic(fmt.Sprintf("[TM][StaticUpdate]Timeout handling static update - TM got stuck somewhere in handleStaticTMUpdate. TxnID: %d.\n", request.TransactionId))
		}
	}(debugChan)*/
	updateArgs := request.Args.(TMStaticUpdateArgs)
	//fmt.Printf("[TM][StaticUpdate]Received %d updates.\n", len(updateArgs.UpdateParams))

	newTxnId := TransactionId(rng.Uint64())
	/*tm.localClock.Lock()
	tm.TxnStartTime[newTxnId] = time.Now().UnixNano()
	tm.localClock.Unlock()*/
	//1st step: discover involved partitions and group updates
	/*fmt.Print("[TM][StaticWrite]Keys received: [")
	for _, currUpdate := range updateArgs.UpdateParams {
		fmt.Print(currUpdate.KeyParams, ", ")
	}
	fmt.Println("]")*/
	//updsPerPartition, reqsPerServer, hasRemote := tm.groupWrites(updateArgs.UpdateParams)
	hasRemote := tm.groupWrites(updateArgs.UpdateParams, updBuf)
	updsPerPartition, reqsPerServer, replyChan, partBitSet, partInfo := updBuf.updsPerPartition, updBuf.reqsPerServer, updBuf.replyChan, updBuf.partitionBitSet, updBuf.prepInfoPerPartition

	//replyChan := make(chan clocksi.Timestamp, len(updsPerPartition))
	//var currRequest MaterializerRequest
	waitFor, args := 0, MaterializerRequest{MatRequestArgs: MatStaticUpdateArgs{TransactionId: newTxnId, ReplyChan: replyChan}}
<<<<<<< Updated upstream
=======
	if len(replyChan) > 0 { //TODO: REMOVE
		panic(fmt.Sprintf("[TM][StaticUpdate]Reply channel has leftover replies from previous invocations. Reply chan len: %d.\n", len(replyChan)))
	}
>>>>>>> Stashed changes
	for i := 0; i < len(updsPerPartition); i++ {
		if partBitSet.GetBit(i) {
			//args.Updates = updsPerPartition[i].ToSlice()
			waitFor++
			//tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: args}, uint64(i))
			tm.mat.SendRequestToChannel(args, uint64(i))
		}
	}

	//2nd step: send update operations to each involved partition
	/*for partId, partUpdates := range updsPerPartition {
		if partUpdates != nil {
			waitFor++
			currRequest = MaterializerRequest{
				MatRequestArgs: MatStaticUpdateArgs{
					TransactionId: newTxnId,
					Updates:       partUpdates,
					ReplyChan:     replyChan,
				},
			}
			tm.mat.SendRequestToChannel(currRequest, uint64(partId))
		}
	}*/

	/*var maxTimestamp clocksi.Timestamp = clocksi.DummyTs
	for i := 0; i < waitFor; i++ {
		reply := <-replyChan
		if reply.Third.IsHigherOrEqual(maxTimestamp) {
			maxTimestamp = reply.Third
		}
		partInfo[reply.First] = tools.Pair[int32, clocksi.Timestamp]{First: reply.Second, Second: reply.Third}
	}*/
	//Partitions may have concurrent prepare clocks due to remote txns. Thus, the right clock is one that maxes every position.
	maxTimestamp := clocksi.NewSliceTimestamp()
	for i := 0; i < waitFor; i++ {
		reply := <-replyChan
		maxTimestamp.MergeInto(reply.Third)
<<<<<<< Updated upstream
		partInfo[reply.First] = tools.Pair[int32, clocksi.Timestamp]{First: reply.Second, Second: reply.Third}
=======
		if !maxTimestamp.IsHigherOrEqual(reply.Third) {
			panic(fmt.Sprintf("[TM][StaticUpdate]Inconsistency in maxTimestamp calculation: merged clk is >= than the prepare clock. Merged: %s, prepare: %s, txnID: %d\n",
				maxTimestamp.ToString(), reply.Third.ToString(), newTxnId))
		}
		partInfo[reply.First] = tools.Pair[int32, clocksi.Timestamp]{First: reply.Second, Second: reply.Third}
	}
	if len(replyChan) > 0 {
		panic(fmt.Sprintf("[TM][StaticUpdate]Reply channel is not empty after receiving all replies: Len of chan left: %d. waitFor: %d.\n", len(replyChan), waitFor))
>>>>>>> Stashed changes
	}

	//Step "2.5" - notify TM's handleCommitReplies() of the number of partitions for this txn.
	tm.commitChan <- TMCommitNPartitions{nPartitions: waitFor, txnId: newTxnId, clk: maxTimestamp}
	//tm.commitChan.Write(TMCommitNPartitions{nPartitions: waitFor, txnId: newTxnId, clk: maxTimestamp})

	//3rd step: send commit to involved partitions
	var wg sync.WaitGroup
	wg.Add(waitFor)
	commitArgs := MatCommitArgs{TransactionId: newTxnId, CommitTimestamp: maxTimestamp, Wg: &wg}
	for i := 0; i < int(nGoRoutines); i++ {
		if partBitSet.GetBit(i) {
			commitArgs.Upds = updsPerPartition[i].ToSlice()
			commitArgs.PosInPrepClks, commitArgs.PrepTimestamp = int(partInfo[i].First), partInfo[i].Second
			tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: commitArgs}, uint64(i))
		}
	}

	//Request for remote operations. Wait for remote commit
	if hasRemote {
		//fmt.Println("[TM][StaticWrite]Has remote upds!!!")
		//fmt.Printf("[TM][StaticUpdate]Starting remote upds. TxnID: %d\n", request.TransactionId)
		tm.handleRemoteStaticUpds(newTxnId, maxTimestamp, reqsPerServer)
		//fmt.Printf("[TM][StaticUpdate]Finished remote upds. TxnID: %d.\n", request.TransactionId)
	}
	wg.Wait() //Note: tm.handleRemoteStaticUpds already waits for the remote replicas.
	//testChannel <- true
	//debugChan <- struct{}{}
	//fmt.Printf("[TM][StaticUpdate]Finished handling static update with %d updates.\n", len(updateArgs.UpdateParams))

	//4th step: send ok to client
	return TMStaticUpdateReply{TransactionId: newTxnId, Timestamp: maxTimestamp, Err: nil}

	/*
		Algorithm:
			1st step: discover involved partitions and group writes
				- for update in writeRequest.UpdateParams
					- getPartitionKey
					- add update to list
			2nd step: send update operations to each involved partition and collect proposed timestamp
				- for each partition involved
					- send list of updates
					- wait for proposed timestamp
					- if proposed timestamp > highest proposed timestamp so far
						highest timestamp = proposed timestamp
			3rd step: send commit to involved partitions
				- for each partition
					- commit(highest timestamp)
			4th step: send ok to client
	*/
}

func (tm *TransactionManager) handleTMReadWithReply(request TransactionManagerRequest, txnPartitions *ongoingTxn, readBuf *tmReadBuffers) {
	request.Args.(TMReadArgs).ReplyChan <- tm.handleTMRead(request, txnPartitions, readBuf)
}

// TODO: Group reads that go for the same partition
func (tm *TransactionManager) handleTMRead(request TransactionManagerRequest, txnPartitions *ongoingTxn, readBuf *tmReadBuffers) (states []crdt.State) {
	readArgs := request.Args.(TMReadArgs)
	tsToUse := request.Timestamp

	/*
		var currReadChan chan crdt.State = nil
		var currRequest MaterializerRequest
		states := make([]crdt.State, len(readArgs.ReadParams))

		//Now, ask to read the client requested version.
		for i, currRead := range readArgs.ReadParams {
			currReadChan = make(chan crdt.State, 1)

			currRequest = MaterializerRequest{
				MatRequestArgs: MatReadArgs{MatReadCommonArgs: MatReadCommonArgs{
					Timestamp:        tsToUse,
					ReadObjectParams: currRead,
					ReplyChan:        currReadChan,
				}, TransactionId: request.TransactionId},
			}
			tm.mat.SendRequest(currRequest)
			states[i] = <-currReadChan
			close(currReadChan)
		}

		readArgs.ReplyChan <- states
		//++fmt.Println(tm.replicaID, "TM - finished handling read.")
	*/

	var currRequest MaterializerRequest
	//readChans := make([]chan crdt.State, len(readArgs.ReadParams))
	if len(readArgs.ReadParams) > len(readBuf.states) {
		readBuf.readChan, readBuf.states = make(chan tools.Pair[int, crdt.State], len(readArgs.ReadParams)), make([]crdt.State, len(readArgs.ReadParams))
	}
	readChan, states := readBuf.readChan, readBuf.states
	//readChan := make(chan tools.Pair[int, crdt.State], len(readArgs.ReadParams))
	//states = make([]crdt.State, len(readArgs.ReadParams))

	var reqsPerServer [][]crdt.ReadObjectParams
	var remoteReqsToChan [][]int
	//reqsPerServer := make([][]crdt.ReadObjectParams, len(tm.remoteIPs))
	//remoteReqsToChan := make([][]int, len(tm.remoteIPs))
	isRemote, serverIndex, hasRemote := true, 0, false

	for i, currRead := range readArgs.ReadParams {
		//readChans[i] = make(chan crdt.State, 1)
		isRemote, serverIndex = tm.getReadLocation(currRead.Bucket)

		if !isRemote {
			currRequest = MaterializerRequest{
				MatRequestArgs: MatReadArgs{MatReadCommonArgs: MatReadCommonArgs{
					Timestamp:        tsToUse,
					ReadObjectParams: currRead,
					ReplyChan:        readChan,
					ReplyIndex:       i,
					//ReplyChan:        readChans[i],
					//HashKey:          new(uint64),
				}, TransactionId: request.TransactionId},
			}
			tm.mat.SendRequest(currRequest)
		} else {
			if !hasRemote { //First time we see a remote read, initialize the slices. Most often, there won't be reads to other replicas, thus the conservative approach.
				reqsPerServer, remoteReqsToChan = make([][]crdt.ReadObjectParams, len(tm.remoteIPs)), make([][]int, len(tm.remoteIPs))
			}
			reqsPerServer[serverIndex] = append(reqsPerServer[serverIndex], currRead)
			remoteReqsToChan[serverIndex] = append(remoteReqsToChan[serverIndex], i)
			hasRemote = true
		}
	}

	if hasRemote {
		go tm.handleRemoteReads(txnPartitions, reqsPerServer, remoteReqsToChan, readChan)
	}

	/*for i, readChan := range readChans {
		states[i] = <-readChan
		close(readChan)
	}*/

	return states
	//++fmt.Println(tm.replicaID, "TM - finished handling read.")
}

func (tm *TransactionManager) handleTMUpdateWithReply(request TransactionManagerRequest, txnPartitions *ongoingTxn, updBuf *tmUpdBuffers) {
	request.Args.(TMUpdateArgs).ReplyChan <- tm.handleTMUpdate(request, txnPartitions, updBuf)
}

func (tm *TransactionManager) handleTMUpdate(request TransactionManagerRequest, txnPartitions *ongoingTxn, updBuf *tmUpdBuffers) (reply TMUpdateReply) {
	//++fmt.Printf("%d TM%d - Started handling update.\n", tm.replicaID, txnPartitions.debugID)
	updateArgs := request.Args.(TMUpdateArgs)

	//updsPerPartition, reqsPerServer, hasRemote := tm.groupWrites(updateArgs.UpdateParams)
	hasRemote := tm.groupWrites(updateArgs.UpdateParams, updBuf)
	updsPerPartition, reqsPerServer, partBitSet := updBuf.updsPerPartition, updBuf.reqsPerServer, updBuf.partitionBitSet

	/*var currRequest MaterializerRequest
	var partId uint64

	for id, partUpdates := range updsPerPartition {
		if partUpdates != nil {
			partId = uint64(id)
			currRequest = MaterializerRequest{
				MatRequestArgs: MatUpdateArgs{
					TransactionId: request.TransactionId,
					Updates:       partUpdates,
				},
			}
			txnPartitions.partitions[partId] = true
			//++fmt.Printf("%d TM%d - Trying to send upds list.\n", tm.replicaID, txnPartitions.debugID)
			tm.mat.SendRequestToChannel(currRequest, partId)
			//++fmt.Printf("%d TM%d - Upds list sent.\n", tm.replicaID, txnPartitions.debugID)
		}
	}*/
	var partId uint64
	txnId := request.TransactionId
	for i := 0; i < len(updsPerPartition); i++ {
		if partBitSet.GetBit(i) {
			partId = uint64(i)
			tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatUpdateArgs{TransactionId: txnId, Updates: updsPerPartition[i].ToSlice()}}, partId)
		}
	}

	if hasRemote {
		tm.handleRemoteUpds(txnPartitions, reqsPerServer)
	}

	updBuf.reset()

	return TMUpdateReply{Success: true, Err: nil}
	//++fmt.Printf("%d TM%d - Finished handling update.\n", tm.replicaID, txnPartitions.debugID)
}

func (tm *TransactionManager) handleNewTrigger(request TransactionManagerRequest) {
	args := request.Args.(TMNewTriggerArgs)
	src, target := args.Source, args.Target

	tm.triggerDB.Lock()
	fmt.Printf("Handling client trigger: %v\n", args)
	if args.IsGeneric {
		matchParams := tm.triggerDB.GetMatchableKeyParams(src.Key, src.Bucket, src.CrdtType)
		tm.triggerDB.AddGenericLink(matchParams, src, target)
	} else {
		tm.triggerDB.AddObject(src.KeyParams)
		tm.triggerDB.AddLink(src, target)
	}
	args.ReplyChan <- true
	tm.triggerDB.DebugPrint("[TM@NewT]")
	tm.triggerDB.Unlock()
	tm.replicator.remote.SendTrigger(AutoUpdate{Trigger: src, Target: target}, args.IsGeneric)
}

func (tm *TransactionManager) handleGetTriggers(request TransactionManagerRequest) {
	args := request.Args.(TMGetTriggersArgs)

	tm.triggerDB.RLock()
	tm.triggerDB.DebugPrint("[TM@GetT]")
	args.ReplyChan <- &tm.triggerDB.TriggerDB
	<-args.WaitFor
	tm.triggerDB.RUnlock()
}

func (tm *TransactionManager) handleGetCRDTWithReply(request TransactionManagerRequest) {
	request.Args.(TMGetCRDTArgs).ReplyChan <- tm.handleGetCRDT(request)
}

func (tm *TransactionManager) handleGetCRDT(request TransactionManagerRequest) (crdts []crdt.CRDT) {
	args := request.Args.(TMGetCRDTArgs)
	crdts = make([]crdt.CRDT, len(args.KeyParams))
	crdtChan := make(chan CRDTPosPair, len(crdts))
	for i, keyParams := range args.KeyParams {
		tm.mat.SendRequest(MaterializerRequest{MatRequestArgs: MatGetCRDTArgs{KeyParams: keyParams, Pos: i, ReplyChan: crdtChan}})
	}
	for i := 0; i < len(args.KeyParams); i++ {
		pair := <-crdtChan
		crdts[pair.Pos] = pair.CRDT
	}
	close(crdtChan)
	return crdts
}

/*
Returns an array in which each index corresponds to one partition.
Associated to each index is the list of reads that belong to the referred partition
(Unused as of now.)
*/
func groupReads(reads []crdt.KeyParams) (readsPerPartition [][]crdt.KeyParams) {
	readsPerPartition = make([][]crdt.KeyParams, nGoRoutines)
	var currChanKey uint64

	for _, read := range reads {
		currChanKey = GetChannelKey(read)
		if readsPerPartition[currChanKey] == nil {
			readsPerPartition[currChanKey] = make([]crdt.KeyParams, 0, len(reads)*2/int(nGoRoutines))
		}
		readsPerPartition[currChanKey] = append(readsPerPartition[currChanKey], read)
	}

	return
}

/*
Returns an array in which each index corresponds to one partition.
Associated to each index is the list of writes that belong to the referred partition
It also separates local updates from updates for objects non-locally replicated
*/
func (tm *TransactionManager) groupWritesNoBuf(updates []crdt.UpdateObjectParams) (updsPerPartition [][]crdt.UpdateObjectParams, reqsPerServer []tools.SliceWithCounter[crdt.UpdateObjectParams], hasRemote bool) {
	updsPerPartition, reqsPerServer = make([][]crdt.UpdateObjectParams, nGoRoutines), make([]tools.SliceWithCounter[crdt.UpdateObjectParams], len(tm.remoteIPs))
	var currChanKey uint64
	isRemote, serverIndex, hasRemote := true, 0, false

	for _, upd := range updates {
		isRemote, serverIndex = tm.getReadLocation(upd.Bucket)

		if !isRemote {
			currChanKey = GetChannelKey(upd.KeyParams)
			if updsPerPartition[currChanKey] == nil {
				updsPerPartition[currChanKey] = make([]crdt.UpdateObjectParams, 0, tools.Min(len(updates)*2/int(nGoRoutines), 1))
			}
			updsPerPartition[currChanKey] = append(updsPerPartition[currChanKey], upd)
		} else {
			//fmt.Printf("[TM][GroupWrites]Found a remote update! Key: %v\n", upd.KeyParams)
			reqsPerServer[serverIndex].Append(upd) //This works even with unitialized SliceWithCounter.
			hasRemote = true
		}
	}

	return
}

func (tm *TransactionManager) groupWrites(updates []crdt.UpdateObjectParams, buf *tmUpdBuffers) (hasRemote bool) {
	localBuf := *buf
	var currChanKey uint64
	if len(localBuf.updsPerPartition) == 0 { //First time the client is doing updates, initialize buf.
		localBuf.updsPerPartition, localBuf.reqsPerServer = make([]tools.SliceWithCounter[crdt.UpdateObjectParams], nGoRoutines), make([]tools.SliceWithCounter[crdt.UpdateObjectParams], len(tm.remoteIPs))
		localBuf.partitionBitSet, localBuf.replyChan = tools.NewBitSet(int(nGoRoutines)), make(chan tools.Triple[int16, int32, clocksi.Timestamp], nGoRoutines)
		localBuf.prepInfoPerPartition = make([]tools.Pair[int32, clocksi.Timestamp], nGoRoutines)
		for i := 0; i < int(nGoRoutines); i++ {
			localBuf.updsPerPartition[i] = tools.NewSliceWithCounter[crdt.UpdateObjectParams](5)
		} /*
			for i := 0; i < len(tm.remoteIPs); i++ {
				localBuf.reqsPerServer[i] = tools.NewSliceWithCounter[crdt.UpdateObjectParams](10)
			}*/
		//We don't initialize remoteIPs as in most common PotionDB usage scenarios, we won't have to forward updates.
		//Regardless, it'll work correctly without initialization.
	}

	for _, upd := range updates {
		isRemote, serverIndex := tm.getReadLocation(upd.Bucket)

		if !isRemote {
			currChanKey = GetChannelKey(upd.KeyParams)
			localBuf.updsPerPartition[currChanKey].Append(upd)
			localBuf.partitionBitSet.Set(int(currChanKey))
		} else {
			localBuf.reqsPerServer[serverIndex].Append(upd)
			hasRemote = true
		}
	}
	*buf = localBuf

	return
}

func (tm *TransactionManager) handleTMStartTxnWithReply(request TransactionManagerRequest, txnPartitions *ongoingTxn, id int, rng *rand.Rand) {
	request.Args.(TMStartTxnArgs).ReplyChan <- tm.handleTMStartTxn(request, txnPartitions, id, rng)
}

func (tm *TransactionManager) handleTMStartTxn(request TransactionManagerRequest, txnPartitions *ongoingTxn, id int, rng *rand.Rand) TMStartTxnReply {
	//++fmt.Printf("%d TM%d - Started handling startTxn.\n", tm.replicaID, txnPartitions.debugID)
	//time.Sleep(15 * time.Second)
	//startTxnArgs := request.Args.(TMStartTxnArgs)

	newClock := tm.getClockToUse(request.Timestamp, id)
	//fmt.Println("[TM]Got clock")
	//txnPartitions.originalClk = newClock.Copy()
	txnPartitions.originalClk = newClock //No need to copy as what's returned by tm.getClockToUse will never be modified.
	txnPartitions.TransactionId = TransactionId(rng.Uint64())

	//Remote data
	//txnPartitions.conns = make([]net.Conn, len(tm.remoteIPs))
	txnPartitions.txnDataToUse = make([][]byte, len(tm.remoteIPs))

	if doCompactHistory {
		tm.ongoingReads[txnPartitions.TransactionId] = tm.clocksArray.Write(newClock)
	}
	//txnPartitions.partSet = makePartSet()
	//txnPartitions.partitions = make([]bool, nGoRoutines)
	//It's already initialized.

	return TMStartTxnReply{TransactionId: txnPartitions.TransactionId, Timestamp: newClock}
	//++fmt.Printf("%d TM%d - Finished handling finishTxn.\n", tm.replicaID, txnPartitions.debugID)
}

func (tm *TransactionManager) handleTMCommitWithReply(request TransactionManagerRequest, txnPartitions *ongoingTxn, id int) {
	request.Args.(TMCommitArgs).ReplyChan <- tm.handleTMCommit(request, txnPartitions, id)
}

func (tm *TransactionManager) handleTMCommit(request TransactionManagerRequest, txnPartitions *ongoingTxn, id int) TMCommitReply {
	//No more reads will happen in this transaction so we can clean the read clock
	tm.clksInUse[id] = clocksi.HighestTs //We assign HighestTs to avoid a race condition with GC known as "torn interface/read", where the interface is not fully nil but the value already is. Highest will always lead to GC ignoring this clk, as intended.

	//++fmt.Printf("%d TM%d - Started handling commit.\n", tm.replicaID, txnPartitions.debugID)
	//commitArgs := request.Args.(TMCommitArgs)

	//PREPARE
	//involvedPartitions := txnPartitions.partSet
	involvedPartitions := txnPartitions.partitions
	//replyChan := make(chan clocksi.Timestamp, nGoRoutines)
	replyChan := make(chan tools.Triple[int16, int32, clocksi.Timestamp], nGoRoutines)
	remoteChan := make(chan bool, 1)
	//if txnPartitions.nConns > 0 {
	if txnPartitions.nTxnsStarted > 0 {
		go tm.handleRemoteCommit(txnPartitions, remoteChan)
	}
	nPartitions := 0

	//Send prepare to each partition involved
	//++fmt.Printf("%d TM%d - Sending prepares to Materializers for id %d.\n", tm.replicaID, txnPartitions.debugID, request.TransactionId)
	//for partId, _ := range involvedPartitions {
	req := MaterializerRequest{MatRequestArgs: MatPrepareArgs{TransactionId: request.TransactionId, ReplyChan: replyChan}}
	//for partId, isOn := range involvedPartitions {
	for partId := 0; partId < int(nGoRoutines); partId++ {
		//if isOn {
		if involvedPartitions.GetBit(partId) {
			nPartitions++
			//++fmt.Printf("%d TM%d - Sending prepare to Materializer %d for id %d.\n", tm.replicaID, txnPartitions.debugID, partId, request.TransactionId)
			tm.mat.SendRequestToChannel(req, uint64(partId))
		}
	}
	//}

	//Collect proposed timestamps and accept the maximum one
	/*var maxTimestamp clocksi.Timestamp = clocksi.DummyTs
	//++fmt.Printf("%d TM%d - Waiting for prepares from Materializers.\n", tm.replicaID, txnPartitions.debugID)
	//Wait for a reply from all partitions, by no order in particular
	for i := 0; i < nPartitions; i++ {
		replyTs := <-replyChan
		if replyTs.Third.IsHigherOrEqual(maxTimestamp) {
			maxTimestamp = replyTs.Third
		}
	}*/
	maxTimestamp := clocksi.NewSliceTimestamp()
	//Wait for a reply from all partitions, by no order in particular
	for i := 0; i < nPartitions; i++ {
		maxTimestamp.MergeInto((<-replyChan).Third)
	}

	//Notify TM's handleCommitReplies() of the number of partitions for this txn.
	tm.commitChan <- TMCommitNPartitions{nPartitions: nPartitions, txnId: request.TransactionId, clk: maxTimestamp}
	//tm.commitChan.Write(TMCommitNPartitions{nPartitions: nPartitions, txnId: request.TransactionId, clk: maxTimestamp})

	//COMMIT
	//Send commit to involved partitions

	//++fmt.Printf("%d TM%d - Sending commits to Materializers.\n", tm.replicaID, txnPartitions.debugID)
	var wg sync.WaitGroup
	wg.Add(nPartitions)
	req = MaterializerRequest{MatRequestArgs: MatCommitArgs{TransactionId: request.TransactionId, CommitTimestamp: maxTimestamp, Wg: &wg}}
	for partId := 0; partId < int(nGoRoutines); partId++ {
		if involvedPartitions.GetBit(partId) {
			tm.mat.SendRequestToChannel(req, uint64(partId))
		}
	}

	if doCompactHistory {
		tm.clocksArray.Delete(tm.ongoingReads[txnPartitions.TransactionId])
		delete(tm.ongoingReads, txnPartitions.TransactionId)
	}
	wg.Wait()

	//if txnPartitions.nConns > 0 {
	if txnPartitions.nTxnsStarted > 0 {
		<-remoteChan //Wait for remote commits to be confirmed.
	}

	txnPartitions.reset()

	//Send ok to client
	return TMCommitReply{Timestamp: maxTimestamp, Err: nil}
	//++fmt.Printf("%d TM%d - Finished handling commit.\n", tm.replicaID, txnPartitions.debugID)
}

func (tm *TransactionManager) handleTMAbort(request TransactionManagerRequest, txnPartitions *ongoingTxn, id int) {
	tm.clksInUse[id] = clocksi.HighestTs //We assign HighestTs to avoid a race condition with GC known as "torn interface/read", where the interface is not fully nil but the value already is. Highest will always lead to GC ignoring this clk, as intended.
	abortReq := MaterializerRequest{MatRequestArgs: MatAbortArgs{TransactionId: request.TransactionId}}
	//for partId, _ := range txnPartitions.partSet {
	for partId := 0; partId < int(nGoRoutines); partId++ {
		if txnPartitions.partitions.GetBit(partId) {
			tm.mat.SendRequestToChannel(abortReq, uint64(partId))
		}
	}
	if doCompactHistory {
		tm.clocksArray.Delete(tm.ongoingReads[txnPartitions.TransactionId])
	}

	//if txnPartitions.nConns > 0 {
	if txnPartitions.nTxnsStarted > 0 {
		remoteChan := make(chan bool, 1)
		tm.handleRemoteAbort(txnPartitions, remoteChan)
		<-remoteChan
	}
	txnPartitions.reset()
}

func (tm *TransactionManager) handleTMBCPerms(bcArgs TMBCPermsArgs) {
	for partId, partPerms := range bcArgs.Perms {
		tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatBCTxnPermissions{
			Values: partPerms, ReplicaID: bcArgs.ReqReplicaID}}, uint64(partId))
	}
}

func (tm *TransactionManager) getClockToUse(clientTs clocksi.Timestamp, id int) (tsToUse clocksi.Timestamp) {
	/*var copyValues []int64
	tm.localClock.Lock()
	copyValues = tm.localClock.Copy()
	tm.localClock.Unlock()
	sortedIDs := clocksi.GetSortedIDs()
	entries := make(map[uint16]int64, len(copyValues))
	for i, id := range sortedIDs {
		entries[id] = copyValues[i]
	}
	copyClk := clocksi.ClockSiTimestamp{VectorClock: entries}*/
	//fmt.Println("[TM]Requesting clock...")
	//tm.localClock.Lock()
	//copyClk := tm.localClock.Copy()
	//copyValues := tm.localClock.FastCopy()
	//tm.localClock.Unlock()
	/*entries := make(map[uint16]int64, len(copyValues))
	for i, id := range tm.replicaIDs {
		entries[id] = copyValues[i]
	}
	copyClk := clocksi.ClockSiTimestamp{VectorClock: entries}*/

	//copyClk := clocksi.ClockSiTimestamp{VectorClock: make(map[uint16]int64, len(tm.replicaIDs))}
	/*copyClk := clocksi.NewSliceTimestamp()
	//test := "hi"
	tm.localClock.Lock()
	tm.localClock.CopyInto(copyClk)
	//test += "ho"
	tm.localClock.Unlock()

	tm.clksInUse[id] = copyClk
	tsToUse = copyClk.Merge(clientTs)
	//fmt.Printf("[TM]Clocks. TM: %s; Merged: %s; Client: %s\n",
	//copyClk.ToSortedString(), tsToUse.ToSortedString(), clientTs.ToSortedString())
	/*if tsToUse.IsEqual(copyClk) {
		//fmt.Println("[TM]TM's clock is higher than client, can return")
		return
	}*/
	clk := tm.localClock.GetClock()
	clientTs.MergeInto(clk)
	tsToUse = clientTs
	tm.clksInUse[id] = tsToUse
	if tsToUse.IsEqual(clk) { //If this is true, basically it means all entries in clientTs were originally <= clk.
		return
	}
	//Have to wait
	//fmt.Println("[TM]Waiting for clock")
	req := TMWaitClock{targetClk: tsToUse, replyChan: make(chan clocksi.Timestamp, 1)} //TODO: What if we only wait on read?
	//tm.clksInUse[id] = tsToUse
	tm.commitChan <- req
	//tm.commitChan.Write(req)
	return <-req.replyChan
}

func (tm *TransactionManager) getReadLocation(bkt string) (isRemote bool, remoteIndex int) {
	if tm.hasAll || bkt == "" {
		//fmt.Println("[ReadLoc]Read for", bkt, "this replica has all or bkt is empty.")
		return false, 0
	}
	for _, localBkt := range tm.ownBuckets {
		if bkt == localBkt {
			//fmt.Println("[ReadLoc]Read for", bkt, "found local matching bucket", localBkt)
			return false, 0
		}
	}
	serverMap, has := tm.bucketToIndex[bkt]
	if !has {
		//Must use a server with *

		//fmt.Println("[ReadLoc]Read for", bkt, "unknown bucket, trying to match with '*'")
		serverMap = tm.bucketToIndex["*"]
	}

	//fmt.Printf("[ReadLoc]Read for %s, sending to first server (%d) of this bucket (was bucket found? %v)\n", bkt, serverMap[0], has)
	return true, serverMap[0]
}

// Temporary method. This is used to avoid compile errors on unused variables
// This unused variables mark stuff that isn't being processed yet.
func ignore(any ...interface{}) {

}

func (tm *TransactionManager) applyRemoteClk(request *TMRemoteClk, wg *sync.WaitGroup) {
	//Can only apply clock if there's no transaction on hold for this clock.
	//start := time.Now()
	//fmt.Printf("[TM][applyRemoteClk]Started applyRemoteClk at %s for ID %d with value %d\n", time.Now().Format("2006-01-02 15:04:05.000"), request.ReplicaID, request.StableTs)
	sortedRemoteID := clocksi.GetSortedPosOfId(request.ReplicaID)
	if tm.downstreamQueue[sortedRemoteID].IsEmpty() {
		wg.Add(int(nGoRoutines))
		tm.mat.SendRequestToAllChannels(MaterializerRequest{MatRequestArgs: MatClkPosUpdArgs{ReplicaID: sortedRemoteID, StableTs: request.StableTs, Wg: wg}})
		wg.Wait()

		tm.remoteClock.UpdatePos(sortedRemoteID, request.StableTs)
		tm.commitChan <- TMCommitReplClkOnly{sortedReplicaID: sortedRemoteID, stableTs: request.StableTs}
		//tm.commitChan.Write(TMCommitReplClkOnly{sortedReplicaID: sortedRemoteID, stableTs: request.StableTs})

		//fmt.Printf("[TM]Remote clk applied. Remote ts: %d. RemoteID: %d. TM clk: %s\n", request.StableTs, request.ReplicaID, tm.localClock.ToSortedString())
		tm.remoteReqsApplied++
		tm.checkPendingRemoteTxns(wg)
	} else {
		//Queue
		//fmt.Printf("[TM]Remote clk queued. Remote ts: %d. RemoteID: %d. TM clk: %s\n", request.StableTs, request.ReplicaID, tm.localClock.ToSortedString())
		tm.downstreamQueue[sortedRemoteID].Append(request)
		tm.nEverQueued++
		tm.inDownQueue++
		panic("[TM]A txn got queued. Not expected.")
	}
	/*end := time.Now()
	fmt.Printf("Finished applyRemoteClk. Took: %dms, at %s for ID %d with value %d\n",
		(end.UnixNano()-start.UnixNano())/int64(time.Millisecond), start.Format("2006-01-02 15:04:05.000"), request.ReplicaID, request.StableTs)*/
}

/*func (tm *TransactionManager) applyRemoteTxnGroup(request *RemoteTxnGroup) {
//I think I can use something similar to what's used for holding txns
//sliceTs := clocksi.FromClockSiToSlice(request.getMinClk())
//replicaPos := sliceTs.GetPosOfId(request.getReplicaID())
//startTs := time.Now().UnixNano() / 1000000
var isLowerOrEqual bool
otherReplicaIDPos := clocksi.GetSortedPosOfId(request.getReplicaID())
//fmt.Printf("[TM]RemoteTxnGroup. (First) Clock received: %s. From ReplicaID: %d. TM clk: %s\n", request.Txns[0].Clk.ToSortedString(), request.getReplicaID(), tm.localClock.ToSortedString())
tm.localClock.Lock()
isLowerOrEqual = request.getMinClk().IsLowerOrEqualExceptFor(tm.localClock.Timestamp, shared.SortedReplicaID, otherReplicaIDPos)
//isLowerOrEqual = tm.localClock.IsHigherOrEqualExceptFor(sliceTs, replicaPos)
tm.localClock.Unlock()
//isLowerOrEqual := true
if isLowerOrEqual {
	//Can apply
	split := tm.splitGroupByPartition(request.Txns)
	//replyChans := make([]chan []crdt.UpdateObjectParams, nGoRoutines)
	//fmt.Printf("[TM]RemoteTxnGroup. Applying remote group.\n")
	//fmt.Printf("[TM][ApplyRemoteTxn][Group]RemoteTxnGroup. Number of txns before split: %d. Clk: %s. Started at: %s\n", len(request.Txns),
	//request.getMaxClk().ToSortedString(), time.Now().Format("2006-01-02 15:04:05.000"))
	for i, txns := range split {
		//replyChan := make(chan []crdt.UpdateObjectParams, 1)
		//replyChans[i] = replyChan
		tm.mat.SendRequestToChannel(MaterializerRequest{
			MatRequestArgs: MatRemoteGroupTxnArgs{Txns: txns, FinalClk: request.getMaxClk(), ReplyChan: tm.matRemoteUpdsChan},
		}, uint64(i))
		/*keys := ""
		for _, txn := range txns {
			for _, upd := range txn.Upds {
				keys += fmt.Sprintf("%+v, ", upd.KeyParams)
			}
		}*/
//fmt.Printf("[TM][ApplyRemoteTxn][Group]Applying update for keys %s\n", keys)
/*}
		tm.processMatRemoteReply(otherReplicaIDPos, request.getMaxClk(), len(split))
		//end := time.Now()
		//fmt.Printf("[TM][RemoteTxn]Finished applying remoteTxnGroup from server %d at %s, took %dms\n", request.getReplicaID(), end.Format("15:04:05.000"), (end.UnixNano()/1000000)-startTs)
	} else {
		//Queue
		//fmt.Printf("[TM]Remote txn group in queue. Remote (first) clk: %s. TM clk: %s.\n", request.getMinClk().ToSortedString(), tm.localClock.Timestamp.ToSortedString())
		tm.downstreamQueue[otherReplicaIDPos].Append(request)
	}
}

func (tm *TransactionManager) splitGroupByPartition(toSplit []RemoteTxn) (split [][]MatRemoteTxn) {
	replicaID, pos := toSplit[0].getReplicaID(), make([]int, nGoRoutines)
	split = make([][]MatRemoteTxn, nGoRoutines)
	for i := 0; i < int(nGoRoutines); i++ {
		split[i] = make([]MatRemoteTxn, len(toSplit))
	}
	for _, txn := range toSplit {
		for partID, partUpds := range txn.Upds {
			split[partID][pos[partID]] = MatRemoteTxn{ReplicaID: replicaID, Timestamp: txn.Clk, Upds: partUpds}
			pos[partID]++
		}
	}
	for i, posValue := range pos {
		split[i] = split[i][:posValue]
	}
	return
}*/

func (tm *TransactionManager) remoteTxnHelper(req RemoteTxn, wg *sync.WaitGroup, nGroupTxns *int, nInChan int) {
	localGroupTxns := *nGroupTxns
	if localGroupTxns >= TM_MAX_TXN_MERGE {
		tm.applyRemoteTxnGroup(wg)
		localGroupTxns = 0
		//Continue execution, as we still need to handle req.
	}
	if localGroupTxns > 0 || (localGroupTxns == 0 && nInChan > 0) {
		sortedRemoteID := clocksi.GetSortedPosOfId(req.getReplicaID())
		if req.Clk.IsLowerOrEqualExceptFor(tm.remoteClock, shared.SortedReplicaID, sortedRemoteID) {
			//Can group. Update remoteClock.
			localGroupTxns++
			for partID, upds := range req.Upds {
				tm.bufTxnsToApply[partID].Append(MatRemoteTxn{ReplicaID: sortedRemoteID, Timestamp: req.Clk, Upds: upds})
			}
			tm.remoteClock.UpdatePos(sortedRemoteID, req.Clk.GetPos(sortedRemoteID))
		} else {
			//Apply whatever is queued and test again. If test succeeds, queue. If test fails, call applyRemoteTxn so that it can get sent to downstreamQueue.
			if localGroupTxns > 0 {
				tm.applyRemoteTxnGroup(wg) //This method will need to clear the buffer also.
				localGroupTxns = 0
				if req.Clk.IsLowerOrEqualExceptFor(tm.remoteClock, shared.SortedReplicaID, sortedRemoteID) {
					//Can group now.
					for partID, upds := range req.Upds {
						tm.bufTxnsToApply[partID].Append(MatRemoteTxn{ReplicaID: sortedRemoteID, Timestamp: req.Clk, Upds: upds})
					}
					localGroupTxns++
					tm.remoteClock.UpdatePos(sortedRemoteID, req.Clk.GetPos(sortedRemoteID))
				} else { //Can't apply still, so we send to queue.
					tm.queueRemoteTxn(req, sortedRemoteID)
				}
			} else { //Couldn't apply before, and there's no group formed yet. Will go to queue.
				tm.queueRemoteTxn(req, sortedRemoteID)
			}
		}
		if nInChan == 0 && localGroupTxns > 0 { //Nothing left in the channel, we better apply what we have.
			tm.applyRemoteTxnGroup(wg)
			localGroupTxns = 0
		}
	} else { //localGroupTxns == 0 && nInChan == 0. Just apply.
		tm.applyRemoteTxn(&req, wg)
	}
	*nGroupTxns = localGroupTxns
}

func (tm *TransactionManager) applyRemoteTxnGroup(wg *sync.WaitGroup) {
	bitset := tools.NewBitSet(int(nGoRoutines))
	copyClk := tm.remoteClock.Copy()
	groupTxnArgs := MatRemoteGroupTxnArgs{FinalClk: copyClk, ReplyChan: tm.matRemoteUpdsChan}
	nWaitFor := 0
	for partID, buf := range tm.bufTxnsToApply {
		if buf.Len() > 0 {
			groupTxnArgs.Txns = buf.ToSlice()
			tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: groupTxnArgs}, uint64(partID))
			nWaitFor++
			bitset.Set(partID)
			tm.bufTxnsToApply[partID].Clear()
		}
	}
	fullClkArgs := MaterializerRequest{MatRequestArgs: MatFullClkUpdArgs{Timestamp: copyClk, Wg: wg}}
	wg.Add(int(nGoRoutines) - nWaitFor) //nWaitFor is the number who received the request.
	if (int(nGoRoutines) - nWaitFor) < 0 {
		panic(fmt.Sprintf("[TM]nGoroutines - nWairFor is negative!!! nGoRoutines: %d, nWaitFor: %d\n", nGoRoutines, nWaitFor))
	}
	nClkSent := 0
	for i := 0; i < int(nGoRoutines); i++ {
		if !bitset.GetBit(i) {
			nClkSent++
			tm.mat.SendRequestToChannel(fullClkArgs, uint64(i))
		}
	}
	if nClkSent != (int(nGoRoutines) - nWaitFor) {
		panic(fmt.Sprintf("[TM]Inconsistency! In RemoteTxnGroup, sent request to %d partitions, and in theory sent clk to %d partitions. But actually, the clk was sent to %d partitions!\n",
			nWaitFor, (int(nGoRoutines) - nWaitFor), nClkSent))
	}

	hasNewDowns := false
	newDowns := make(map[uint64][]crdt.UpdateObjectParams) //uint64: partitionID
	for i := 0; i < nWaitFor; i++ {
		reply := <-tm.matRemoteUpdsChan
		if len(reply.Second) > 0 {
			hasNewDowns = true
			newDowns[uint64(reply.First)] = reply.Second
		}
	}
	wg.Wait()

	tm.commitChan <- TMCommitReplFullClk{clk: copyClk} //We may have updated several positions.
	if hasNewDowns {
		tm.downstreamOpsCh <- TMTxnForRemote{ops: newDowns} //Sending all grouped
	}
}

func (tm *TransactionManager) applyRemoteTxn(request *RemoteTxn, wg *sync.WaitGroup) {
	//May have to put the transaction on hold. An hold only for remote transactions.
	//fmt.Printf("[TM]RemoteTxn. Clock received: %s. From ReplicaID: %d.\n", request.Clk.ToSortedString(), request.getReplicaID())
	remoteSortedID := clocksi.GetSortedPosOfId(request.getReplicaID())

	//Can apply
	if request.Clk.IsLowerOrEqualExceptFor(tm.remoteClock, shared.SortedReplicaID, remoteSortedID) {
		//fmt.Printf("[TM]Starting to apply remote txn with clk %s.\n", request.Clk.ToSortedString())
		//In theory we don't need to update all partition's clock now, as at the end of a sequence of remoteTxns there always comes a remoteClk message.
		//In practice, there's a trade-off here:
		//1 - if we update on every txn, we have more overhead on all partitions. However, this gives more txn grouping opportunity in the Replicator later on.
		//2 - if we only update at the end, less overhead with the partitions, but (maybe) several txns will have very different clocks, thus reducing grouping?
		//Actually if we update on every txn... doesn't that also create many small txns that can't be grouped?
		//Let's try updating on every txn and see how it goes.
		//fmt.Printf("[TM][RemoteTxn]Remotetxn from server %d. Clk: %s. Started at: %s\n",
		//	request.getReplicaID(), request.Clk.ToSortedString(), time.Now().Format("2006-01-02 15:04:05.000"))
		bitset := tools.NewBitSet(int(nGoRoutines))
		for i, upds := range request.Upds {
			//replyChan := make(chan []crdt.UpdateObjectParams, 1)
			//replyChans[i] = replyChan
			tm.mat.SendRequestToChannel(MaterializerRequest{
				MatRequestArgs: MatRemoteTxnArgs{MatRemoteTxn: tm.makeMatRemoteTxn(remoteSortedID, request.Clk, upds), ReplyChan: tm.matRemoteUpdsChan},
			}, uint64(i))
			bitset.Set(i)
			//fmt.Printf("[TM][ApplyRemoteTxn][Single]Applying update for keys %s\n", keys)
		}
		//fmt.Printf("[TM][applyRemoteTxn]Will send clk to %d partitions. NGoRoutines: %d. Number of partitions with updates: %d.\n", int(nGoRoutines)-len(request.Upds), nGoRoutines, len(request.Upds))
		wg.Add(int(nGoRoutines) - len(request.Upds))
		nClkSent := 0
		clkReq := MaterializerRequest{MatRequestArgs: MatClkPosUpdArgs{ReplicaID: remoteSortedID, StableTs: request.Clk.GetPos(remoteSortedID), Wg: wg}}
		for i := 0; i < int(nGoRoutines); i++ {
			if !bitset.GetBit(i) {
				tm.mat.SendRequestToChannel(clkReq, uint64(i))
				nClkSent++
			}
		}

		if nClkSent != (int(nGoRoutines)-len(request.Upds)) || (int(nGoRoutines)-len(request.Upds)) < 0 {
			panic(fmt.Sprintf("[TM]Inconsistency! In RemoteTxn, sent request to %d partitions, and in theory sent clk to %d partitions. But actually, the clk was sent to %d partitions!\n",
				len(request.Upds), (int(nGoRoutines) - len(request.Upds)), nClkSent))
		}
		tm.remoteReqsApplied++
		tm.processMatRemoteReply(remoteSortedID, request.Clk, len(request.Upds), wg)
		//end := time.Now()
		//fmt.Printf("[TM][RemoteTxn]Finished applying remoteTxn from server %d at %s.\n", request.getReplicaID(), time.Now().Format("2006-01-02 15:04:05.000"))
		//fmt.Printf("[TM][RemoteTxn]Finished applying remoteTxn from server %d at %s, took %dms\n", request.getReplicaID(), end.Format("15:04:05.000"), (end.UnixNano()/1000000)-startTs)
	} else {
		tm.nEverQueued++
		tm.inDownQueue++
		//Queue
		//Good thing is, for each ID, we will receive the transactions in order.
		//fmt.Printf("[TM][RemoteTxn]Remote txn in queue. Clk received: %s. TM remote clk: %s. TM main clk: %s.\n", request.Clk.ToSortedString(), tm.remoteClock.ToSortedString(), tm.localClock.GetClock().ToSortedString())
		tm.downstreamQueue[remoteSortedID].Append(request)
		panic(fmt.Sprintf("[TM]A txn got queued. Not expected. Txn clock: %s. Local remote clk: %s. Sender replicaID: %d. Our replicaID: %d", request.Clk.ToSortedString(), tm.remoteClock.ToSortedString(), request.getReplicaID(), tm.replicaID))
	}
}

// Pre: the txn needs to be queue, i.e., the clock was already tested before calling this method.
func (tm *TransactionManager) queueRemoteTxn(request RemoteTxn, remoteSortedID uint16) {
	tm.downstreamQueue[remoteSortedID].Append(request)
	tm.nEverQueued++
	tm.inDownQueue++
	panic(fmt.Sprintf("[TM]A txn got queued. Not expected. Txn clock: %s. Local remote clk: %s. Sender replicaID: %d. Our replicaID: %d", request.Clk.ToSortedString(), tm.remoteClock.ToSortedString(), request.getReplicaID(), tm.replicaID))
}

func (tm *TransactionManager) processMatRemoteReply(posReplicaID uint16, clk clocksi.Timestamp, nParts int, wg *sync.WaitGroup) {
	newDowns := make(map[uint64][]crdt.UpdateObjectParams) //int: partitionID
	hasNewDowns := false
	//fmt.Printf("[TM][ApplyRemoteTxn]Starting to wait for materializer replies. SenderID: %d. Clk: %s. Time: %s\n", posReplicaID, clk.ToSortedString(), time.Now().Format("2006-01-02 15:04:05.000"))
	//Receive replies; check if there's any new downstream.
	/*for i, channel := range replyChans {
		if channel != nil {
			reply := <-channel
			if len(reply) > 0 {
				hasNewDowns = true
				newDowns[uint64(i)] = reply
			}
		}
	}*/
	for i := 0; i < nParts; i++ {
		reply := <-tm.matRemoteUpdsChan
		if len(reply.Second) > 0 {
			hasNewDowns = true
			newDowns[uint64(reply.First)] = reply.Second //OK, as reply.Second is already a copy (materializer copies the buffer to a new buffer when replying.)
		}
	}
	wg.Wait() //Wait for the clock update partitions.

	/*posOfReplica := tm.localClock.GetPosOfId(replicaID)
	var copyValues []int64
	newValue := clk.GetPos(replicaID)
	tm.localClock.Lock()
	tm.localClock.UpdatePos(posOfReplica, newValue)
	copyValues = tm.localClock.Copy()
	tm.localClock.Unlock()
	//copyClk := clocksi.FromSortedSliceToClockSi(copyValues)*/

	/*copyClkValues := make([]int64, len(tm.replicaIDs))
	tm.localClock.Lock()
	tm.localClock.Timestamp.UpdatePos(replicaID, clk.GetPos(replicaID))
	tm.localClock.Timestamp.FastCopyInto(copyClkValues)
	tm.localClock.Unlock()
	copyClk := clocksi.FromSliceValuesToClockSiTimestamp(copyClkValues)*/
	/*copyClk := clocksi.NewSliceTimestamp()
	updValue := clk.GetPos(posReplicaID)
	tm.localClock.Lock()
	tm.localClock.Timestamp.UpdatePos(posReplicaID, updValue)
	tm.localClock.Timestamp.CopyInto(copyClk)
	tm.localClock.Unlock()*/
	updValue := clk.GetPos(posReplicaID)
	tm.remoteClock.UpdatePos(posReplicaID, updValue)
	tm.commitChan <- TMCommitReplClk{sortedReplicaID: posReplicaID, stableTs: updValue}
	//tm.commitChan.Write(TMCommitReplClk{sortedReplicaID: posReplicaID, stableTs: updValue})

	/*tm.localClock.Lock()
	tm.localClock.Timestamp.UpdatePos(replicaID, clk.GetPos(replicaID))
	copyClk := tm.localClock.Timestamp.Copy()
	tm.localClock.Unlock()*/

	//fmt.Printf("[TM][ApplyRemoteTxn]Current time @ end of remoteReply remotes for serverID %d, clk %s: %s\n", posReplicaID, clk.ToSortedString(), time.Now().Format("2006-01-02 15:04:05.000"))
	if hasNewDowns {
		tm.downstreamOpsCh <- TMTxnForRemote{ops: newDowns}
	}
	tm.checkPendingRemoteTxns(wg)
}

func (tm *TransactionManager) checkPendingRemoteTxns(wg *sync.WaitGroup) {
	if tm.inDownQueue == 0 {
		return
	}
	//Idea (I think somewhat similar to the previous one): go through requests until nothing can be applied
	//Steps:
	//Repeats
	//2 - Search if any ID can be applied. if it can, queue everything of that ID that can be applied. Update the clock.
	//3 - Keep doing the search, until a full cycle is done without any findings.
	//End of repeats
	//4 - Execute everything. At the end, send a clock update to every partition
	//5 - Wait for all partitions to finish commiting
	//6 - Update the clock.
	//The idea is that I can send a big request to the materializer and avoid a lot of the overhead.
	//This is "cheap" to do as this is a separate thread that is doing all the work gathering, so does not affect ongoing transactions.
	//This version now uses a re-usable buffer to hold the txns, avoiding some GC/allocation overhead.
	txnBuf := tm.bufPendingRemoteTxns
	var currMsgSlice []TMRemoteMsg

	atLeastOne := false    //Keeps track if there's at least one txn or clock to apply
	foundSomething := true //For as long as one transaction of any replica is found to be appliable, the external cycle can continue
	var sortedReplicaID uint16

	//fmt.Printf("[TM][checkPendingRemoteTxns]Start at tm remote clk %s, time %s.\n", tm.remoteClock.ToSortedString(), time.Now().Format("2006-01-02 15:04:05.000"))

	for foundSomething {
		foundSomething = false
		for remoteID, msgsBuf := range tm.downstreamQueue {
			sortedReplicaID = uint16(remoteID)
			if msgsBuf.Len() > 0 { //Skip our replicaID and any other empty queue.
				currMsgSlice = msgsBuf.ToSlice()
				for _, req := range currMsgSlice {
					switch typedReq := req.(type) {
					case RemoteTxn:
						if typedReq.Clk.IsLowerOrEqualExceptFor(tm.remoteClock, shared.SortedReplicaID, sortedReplicaID) {
							//Safe to commit. Add to list. Update copyClk
							tm.remoteClock.UpdatePos(sortedReplicaID, typedReq.Clk.GetPos(sortedReplicaID))
							for i, upds := range typedReq.Upds {
								txnBuf[i].Append(tm.makeMatRemoteTxn(sortedReplicaID, typedReq.Clk, upds))
							}
							foundSomething, atLeastOne = true, true
							msgsBuf.HideHead()
							tm.inDownQueue--
						} else {
							//Need to go to the next replica.
							break
						}
					case TMRemoteClk:
						tm.remoteClock.UpdatePos(sortedReplicaID, typedReq.StableTs)
						foundSomething, atLeastOne = true, true
						msgsBuf.HideHead()
						tm.inDownQueue--
					}
				}
			}
		}
	}

	if !atLeastOne { //Nothing to do (i.e., can't apply anything), return early.
		//fmt.Printf("[TM][checkPendingRemoteTxns]Nothing pending that can be applied. Remote clk %s, time %s.\n", tm.remoteClock.ToSortedString(), time.Now().Format("2006-01-02 15:04:05.000"))
		return
	}
	fmt.Printf("[TM][checkPendingRemoteTxns]Applying some txns/reqs that were in queue.\n")
	tm.remoteReqsApplied++
	copyClk := tm.remoteClock.Copy() //Copy this clock as it will be sent to the partitions, thus it may be stored by CRDTs.
	//To every partition, send a "big" request with all the transactions that were on hold + clock update.
	nWaitFor := 0
	bitset := tools.NewBitSet(int(nGoRoutines))
	for i, reqs := range txnBuf {
		if !reqs.IsEmpty() { //Some partitions may not be involved.
			tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatRemoteGroupTxnArgs{
				Txns:      reqs.ToSlice(),
				FinalClk:  copyClk,
				ReplyChan: tm.matRemoteUpdsChan,
			}}, uint64(i))
			nWaitFor++
			bitset.Set(i)
		}
	}
	fullClkArgs := MaterializerRequest{MatRequestArgs: MatFullClkUpdArgs{Timestamp: copyClk, Wg: wg}}
	wg.Add(int(nGoRoutines) - nWaitFor) //nWaitFor is the number who received the request.
	for i := 0; i < int(nGoRoutines); i++ {
		if !bitset.GetBit(i) {
			tm.mat.SendRequestToChannel(fullClkArgs, uint64(i))
		}
	}

	//Take this opportunity while we wait for the partitions to deep clean the hidden parts of downstreamQueue buffers.
	for i, buf := range tm.downstreamQueue {
		hiddenHead := buf.LenHiddenHead()
		if hiddenHead > 0 && hiddenHead > buf.Len()/10 { //We only shift if the amount of entries left doesn't far exceed the hidden section, to avoid expensive copying.
			buf.ShiftElementsLeft()
			tm.downstreamQueue[i] = buf
		}
	}
	//Do a shallow clean of bufPendingRemoteTxns (OK-ish as this will be overwritten by future queued txns).
	//This is safe as it simply resets the start and len variables, thus not affecting the slices sent to the partitions. We can't deep clear though.
	for i := range txnBuf {
		txnBuf[i].Clear()
	}
	tm.bufPendingRemoteTxns = txnBuf

	hasNewDowns := false
	newDowns := make(map[uint64][]crdt.UpdateObjectParams) //uint64: partitionID
	for i := 0; i < nWaitFor; i++ {
		reply := <-tm.matRemoteUpdsChan
		if len(reply.Second) > 0 {
			hasNewDowns = true
			newDowns[uint64(reply.First)] = reply.Second
		}
	}
	wg.Wait()

	/*tm.localClock.Lock()
	tm.localClock.MergeInto(copyClk)
	tm.localClock.Unlock()*/
	//tm.remoteClk was already full updated.
	tm.commitChan <- TMCommitReplFullClk{clk: copyClk} //We may have updated several positions.
	//tm.commitChan.Write(TMCommitReplFullClk{clk: copyClk}) //We may have updated several positions.
	if hasNewDowns {
		tm.downstreamOpsCh <- TMTxnForRemote{ops: newDowns} //Sending all grouped
	}
	if len(tm.remoteChan) == 0 { //Opportunity to deep clean bufPendingRemoteTxns.
		for i := range tm.bufPendingRemoteTxns {
			tm.bufPendingRemoteTxns[i].DeepClear()
		}
	}
	//fmt.Printf("[TM][checkPendingRemoteTxns]End at tm remote clk %s, time %s.\n", tm.remoteClock.ToSortedString(), time.Now().Format("2006-01-02 15:04:05.000"))
}

/*func (tm *TransactionManager) checkPendingRemoteTxns(copyClk clocksi.Timestamp) {
//Idea (I think somewhat similar to the previous one): go through requests until nothing can be applied
//Steps:
//Repeats
//2 - Search if any ID can be applied. if it can, queue everything of that ID that can be applied. Update the clock.
//3 - Keep doing the search, until a full cycle is done without any findings.
//End of repeats
//4 - Execute everything. At the end, send a clock update to every partition
//5 - Wait for all partitions to finish commiting
//6 - Update the clock.
//The idea is that I can send a big request to the materializer and avoid a lot of the overhead.
//This is "cheap" to do as this is a separate thread that is doing all the work gathering, so does not affect ongoing transactions.

//fmt.Println("[TM]Pending check")
//The structure to store can be something like... per partition? I still need to have txns separate for VM purposes.
reqsPerPart := make([][]MatRemoteTxn, nGoRoutines)
//replyChans := make([]chan []crdt.UpdateObjectParams, nGoRoutines)
for i := range reqsPerPart {
	reqsPerPart[i] = make([]MatRemoteTxn, 0, 10)
	//replyChans[i] = make(chan []crdt.UpdateObjectParams, 1)
}
newDowns := make(map[uint64][]crdt.UpdateObjectParams) //int: partitionID

atLeastOne := false    //Keeps track if there's at least one txn or clock to apply
foundSomething := true //For as long as one transaction of any replica is found to be appliable, the external cycle can continue
posToHide := 0         //Auxiliary variable that states until which point requests were processed for a given remoteID.
var currReplicaSortedID, origReplicaID uint16

//startTs := time.Now().UnixNano()
//Gather list of txns that can be applied
for foundSomething {
	foundSomething = false
	for remoteID, msgs := range tm.downstreamQueue {
		posToHide, currReplicaSortedID, origReplicaID = 0, uint16(remoteID), clocksi.GetPosFromSortedPos(uint16(remoteID))
		for _, req := range msgs {
			switch typedReq := req.(type) {
			case RemoteTxn:
				if typedReq.Clk.IsLowerOrEqualExceptFor(copyClk, shared.SortedReplicaID, currReplicaSortedID) {
					//Safe to commit. Add to list. Update copyClk
					copyClk.UpdatePos(currReplicaSortedID, typedReq.Clk.GetPos(currReplicaSortedID))
					for i, upds := range typedReq.Upds {
						reqsPerPart[i] = append(reqsPerPart[i], tm.makeMatRemoteTxn(origReplicaID, typedReq.Clk, upds))
					}
					foundSomething, atLeastOne = true, true
					posToHide++
				} else {
					//Need to go to next replica.
					break
				}
			case RemoteTxnGroup:
				if typedReq.getMaxClk().IsLowerOrEqualExceptFor(copyClk, shared.SortedReplicaID, currReplicaSortedID) {
					//Safe to commit. Add to list. Update copyClk
					copyClk.UpdatePos(currReplicaSortedID, typedReq.getMaxClk().GetPos(currReplicaSortedID))
					for _, txn := range typedReq.Txns {
						for i, upds := range txn.Upds {
							reqsPerPart[i] = append(reqsPerPart[i], tm.makeMatRemoteTxn(origReplicaID, txn.Clk, upds))
						}
					}
					foundSomething, atLeastOne = true, true
					posToHide++
				} else {
					//Need to go to next replica.
					break
				}
			case TMRemoteClk:
				copyClk.UpdatePos(currReplicaSortedID, typedReq.StableTs)
				foundSomething, atLeastOne = true, true
				posToHide++
			}
		}
		for i := 0; i < posToHide; i++ {
			//For GC purposes
			msgs[i] = nil
		}
		if posToHide == len(msgs) { //Empty, so we can start writing from the beggining

		}
		tm.downstreamQueue[remoteID] = msgs[posToHide:]
	}
}
//fmt.Println("[TM]Pending check end")

if !atLeastOne {
	//Nothing to apply, can return
	return
}
//To every partition, send a "big" request with all the transactions that were on hold + clock update.
for i, reqs := range reqsPerPart {
	tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatRemoteGroupTxnArgs{
		Txns:     reqs,
		FinalClk: copyClk,
		//ReplyChan: replyChans[i],
		ReplyChan: tm.matRemoteUpdsChan,
	}}, uint64(i))
}

hasNewDowns := false
//Wait for replies and update the clock here.
/*for i, replyChan := range replyChans {
	reply := <-replyChan
	if len(reply) > 0 {
		hasNewDowns = true
		newDowns[uint64(i)] = reply
	}
}*/ /*
	for i := 0; i < len(reqsPerPart); i++ {
		reply := <-tm.matRemoteUpdsChan
		if len(reply.Second) > 0 {
			hasNewDowns = true
			newDowns[uint64(reply.First)] = reply.Second
		}
	}

	//copySliceClk := clocksi.FromClockSiToSlice(copyClk)
	tm.localClock.Lock()
	tm.localClock.MergeInto(copyClk)
	//tm.localClock.SliceTimestamp.Merge(copySliceClk)
	tm.localClock.Unlock()

	/*tm.localClock.Lock()
	tm.localClock.Timestamp = tm.localClock.Merge(copyClk)
	//fmt.Println("[TM]Current time @ end of checking pending remotes:", time.Now().Format("2006-01-02 15:04:05.000"))
	tm.localClock.Unlock()*/ /*

	if hasNewDowns {
		tm.downstreamOpsCh <- TMTxnForRemote{ops: newDowns} //Sending all grouped
	}
	//end := time.Now()
	//fmt.Printf("[TM][PendingCheck]Finished applying pending txns at %s, took %dms\n", end.Format("15:04:05.000"), (end.UnixNano()/1000000)-startTs)
	//TODO: Forced GC for when downstreamQueue grows too big in capacity? Like make new slices.
}*/

func (tm *TransactionManager) makeMatRemoteTxn(id uint16, clk clocksi.Timestamp, upds []crdt.UpdateObjectParams) MatRemoteTxn {
	return MatRemoteTxn{ReplicaID: id, Timestamp: clk, Upds: upds}
}

func (tm *TransactionManager) handleTMGetSnapshot(snapshot *TMGetSnapshot) {
	buckets, replChan := snapshot.Buckets, snapshot.ReplyChan

	//var values []int64
	/*tm.localClock.Lock()
	//values = tm.localClock.Copy()
	tsToUse := tm.localClock.Timestamp.Copy()
	tm.localClock.Unlock()*/
	//tsToUse := clocksi.FromSortedSliceToClockSi(values)
	tsToUse := tm.localClock.GetClock()
	nParts := len(tm.mat.channels)

	//Ask to read snapshots based on the localClock.
	replyChans := make([]chan []*proto.ProtoCRDT, nParts)
	for i := range replyChans {
		channel := make(chan []*proto.ProtoCRDT)
		replyChans[i] = channel
		tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatGetSnapshotArgs{
			Timestamp: tsToUse, Buckets: buckets, ReplyChan: channel}}, uint64(i))
	}
	partStates := make([][]*proto.ProtoCRDT, nParts)
	for i, channel := range replyChans {
		partStates[i] = <-channel
		close(channel)
	}

	replChan <- TMGetSnapshotReply{Timestamp: tsToUse, PartStates: partStates}
}

func (tm *TransactionManager) handleTMApplySnapshot(snapshot *TMApplySnapshot) {
	ts, states := snapshot.Timestamp, snapshot.PartStates
	//posToUse := tm.localClock.GetPosOfId(tm.replicaID)
	posToUse := clocksi.GetSortedPosOfId(tm.replicaID)
	/*tm.localClock.Lock()
	//ts.UpdatePos(tm.replicaID, tm.localClock.GetPosValue(posToUse))
	ts.UpdatePos(posToUse, tm.localClock.GetPos(posToUse))
	tm.localClock.Unlock()*/
	ts.UpdatePos(posToUse, tm.localClock.GetValue(posToUse))

	for i, partState := range states {
		tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatApplySnapshotArgs{
			Timestamp: ts, ProtoCRDTs: partState}}, uint64(i))
	}

	//TODO: Need to update remote entries at the end of this... or right at start?
	//sliceTs := clocksi.FromClockSiToSlice(ts)
	/*tm.localClock.Lock()
	tm.localClock.Timestamp = tm.localClock.Merge(ts)
	//tm.localClock.SliceTimestamp.Merge(sliceTs)
	tm.localClock.Unlock()*/
	tm.commitChan <- TMCommitReplFullClk{clk: ts} //We may have updated several positions.
	//tm.commitChan.Write(TMCommitReplFullClk{clk: ts}) //We may have updated several positions.
}

func (tm *TransactionManager) handleReplicaID(replica *TMReplicaID) {
	remoteID := replica.ReplicaID
	if _, has := tm.RemoteInfo.remoteIDToIndex[remoteID]; !has {
		fmt.Println("[TM]Adding replicaID", remoteID, "at", time.Now().Format("15:04:05.000"), "via RabbitMQ.")
		clocksi.AddNewID(remoteID)
		tm.RemoteInfo.remoteBks = append(tm.RemoteInfo.remoteBks, replica.Buckets)
		tm.RemoteInfo.remoteIPs = append(tm.RemoteInfo.remoteIPs, replica.IP)
		tm.RemoteInfo.remoteIDToIndex[remoteID] = len(tm.RemoteInfo.remoteIPs) - 1
	} //else: ignore, already received ID via S2S
}

func (tm *TransactionManager) handleRemoteTrigger(trigger *TMRemoteTrigger) {
	tm.triggerDB.Lock()
	fmt.Printf("[TM]Handling remote trigger: %v\n", *trigger)
	if trigger.IsGeneric {
		src := trigger.Trigger
		matchable := tm.triggerDB.GetMatchableKeyParams(src.Key, src.Bucket, src.CrdtType)
		tm.triggerDB.AddGenericLink(matchable, src, trigger.Target)
	} else {
		tm.triggerDB.AddLink(trigger.AutoUpdate.Trigger, trigger.AutoUpdate.Target)
	}
	tm.triggerDB.Unlock()
}

// This code is run before the server starts accepting client requests, so it doesn't need to be efficient.
func (tm *TransactionManager) handleTMStart(start *TMStart) {
	if shared.IsReplDisabled && len(othersIPList) > 0 { //Ignore the TMStart from Replicator, wait for S2S.

	} else if tm.replicaIDs == nil {
		tm.finishTMInitialialization()
		tm.waitStartChan <- BOTH_READY
	} else { //Initialization might have already been finished by S2S replicaID sharing.
		tm.waitStartChan <- REPL_READY
	}
}

// This can only be done after we know all replicaIDs.
func (tm *TransactionManager) finishTMInitialialization() {
	ids := clocksi.GetCopyKeys()
	clocksi.SetSortedIDs(ids)
	shared.SortedReplicaID = clocksi.GetSortedPosOfId(tm.replicaID) //Set the shared variable for the sorted replica ID.
	tm.downstreamQueue = make([]tools.SliceWithHideable[TMRemoteMsg], len(ids))
	for i := uint16(0); i < uint16(len(ids)); i++ {
		if i != shared.SortedReplicaID { //We don't need a buffer for ourselves.
			tm.downstreamQueue[i] = tools.NewSliceWithHideable[TMRemoteMsg](DOWN_QUEUE_STARTING_LEN)
		}
	}
	//tm.localClock.SliceTimestamp = clocksi.NewSliceTimestamp()
	newClk := clocksi.Timestamp(clocksi.NewSliceTimestamp())
	tm.localClock.readPtr.Store(&newClk)
	tm.remoteClock = newClk.Copy()
	go tm.handleCommitReplies()
	//tm.mat.SendRequestToAllChannels(MaterializerRequest{MatRequestArgs: MatWaitForReplicasArgs{}})
	tm.mat.NotifyClkReady()
	for i, buckets := range tm.remoteBks {
		for _, bkt := range buckets {
			tm.bucketToIndex[bkt] = append(tm.bucketToIndex[bkt], i)
		}
	}
	if doesJoin { //In this case, can only initialize connPool here.
		tm.connPool = initializeConnPool(tm.remoteIPs)
	} else { //Finish optimistic initialization with the real IPs.
		tm.connPool.finishOptimisticInitializationConnPool(tm.remoteIPs)
	}
	//tm.connPool = initializeConnPool(tm.remoteIPs)
	tm.remoteBks = nil
	fmt.Println("[TM][Start]RemoteIPs:", tm.remoteIPs)
	StartBCTimer(tm.mat, tm.connPool, tm.remoteIDToIndex, tm.replicaID)
	tm.replicaIDs = ids
}

func (tm *TransactionManager) handleDownstreamGeneratedOps() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	//replyChan := make(chan clocksi.Timestamp, nGoRoutines)
	replyChan := make(chan tools.Triple[int16, int32, clocksi.Timestamp], nGoRoutines)
	partInfo := make([]tools.Pair[int32, clocksi.Timestamp], nGoRoutines)
	nOps, nReqs := 0, 0
	for {
		req := <-tm.downstreamOpsCh
		//fmt.Println("[TM]HandleDownstreamGeneratedOps. Downstream generated OPs at", time.Now().Format("2006-01-02 15:04:05.000"))
		newTxnId := TransactionId(rng.Uint64())
		//replyChan := make(chan clocksi.Timestamp, len(req.ops))
		matReq := MaterializerRequest{MatRequestArgs: MatPrepareForRemoteArgs{TransactionId: newTxnId, ReplyChan: replyChan}}
		//Send prepare
		for partId := range req.ops {
			tm.mat.SendRequestToChannel(matReq, partId)
		}
		/*for partId, partUpds := range req.ops {
			tm.mat.SendRequestToChannel(MaterializerRequest{
				MatRequestArgs: MatPrepareForRemoteArgs{TransactionId: newTxnId, Updates: partUpds, ReplyChan: replyChan},
			}, partId)
		}*/

		//TODO: Remove. Checking upds.
		/*for partID, ops := range req.ops {
			for i, upd := range ops {
				if _, ok := upd.UpdateArgs.(crdt.ProtoDownUpd); !ok {
					fmt.Printf("[TM][downGen]Received non-proto down update for part %d, index %d: Key %v, Upd: (%T) %+v\n",
						partID, i, upd.KeyParams, upd.UpdateArgs, upd.UpdateArgs)
				}
			}
		}*/

<<<<<<< Updated upstream
		var maxTimestamp clocksi.Timestamp = clocksi.DummyTs
=======
		/*var maxTimestamp clocksi.Timestamp = clocksi.DummyTs
>>>>>>> Stashed changes
		//Wait for reply of each partition
		for i := 0; i < len(req.ops); i++ {
			reply := <-replyChan
			if reply.Third.IsHigherOrEqual(maxTimestamp) {
				maxTimestamp = reply.Third
			}
			partInfo[reply.First] = tools.Pair[int32, clocksi.Timestamp]{First: reply.Second, Second: reply.Third}
<<<<<<< Updated upstream
=======
		}*/
		maxTimestamp := clocksi.NewSliceTimestamp()
		for i := 0; i < len(req.ops); i++ {
			reply := <-replyChan
			maxTimestamp.MergeInto(reply.Third)
			if !maxTimestamp.IsHigherOrEqual(reply.Third) {
				panic(fmt.Sprintf("[TM][StaticUpdate]Inconsistency in maxTimestamp calculation: merged clk is >= than the prepare clock. Merged: %s, prepare: %s, txnID: %d\n",
					maxTimestamp.ToString(), reply.Third.ToString(), newTxnId))
			}
			partInfo[reply.First] = tools.Pair[int32, clocksi.Timestamp]{First: reply.Second, Second: reply.Third}
>>>>>>> Stashed changes
		}

		//Notify TM's handleCommitReplies() of the number of partitions for this txn.
		tm.commitChan <- TMCommitNPartitions{nPartitions: len(req.ops), txnId: newTxnId, clk: maxTimestamp}
		//tm.commitChan.Write(TMCommitNPartitions{nPartitions: len(req.ops), txnId: newTxnId, clk: maxTimestamp})

		//Send commit to involved partitions
		/*commitReq := MaterializerRequest{MatRequestArgs: MatCommitArgs{TransactionId: newTxnId, CommitTimestamp: maxTimestamp}}
		for partId := range req.ops {
			tm.mat.SendRequestToChannel(commitReq, partId)
		}*/
		commitArgs := MatCommitArgs{TransactionId: newTxnId, CommitTimestamp: maxTimestamp, CommitType: NU_FOR_REMOTE_COMMIT}
		for partId := range req.ops {
			commitArgs.Upds, commitArgs.PosInPrepClks, commitArgs.PrepTimestamp = req.ops[partId], int(partInfo[partId].First), partInfo[partId].Second
			tm.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: commitArgs}, partId)
			nOps += len(req.ops[partId])
		}
		nReqs++
		if nReqs&63 == 0 {
			fmt.Printf("[TM][downGen]Received %d downstream requests so far, %d ops total, at %s.\n", nReqs, nOps, time.Now().Format("2006-01-02 15:04:05.000"))
		}
		//The partitions will request the clock to be updated.
		//fmt.Println("[TM][downGen]Commited txn with NuCRDTs for other replicas with clk", maxTimestamp.ToSortedString())
	}
}

/*func (tm *TransactionManager) doHistoryCompact() {
	oldestRead := tm.clocksArray.ReadFirst()
	if oldestRead == nil {
		//TODO: Clean everything until last commited clock
	} else {
		//oldestTxnClk := oldestRead.(clocksi.Timestamp)
		//TODO: Send requests to clean history
	}
}*/

//Remote handling functions

// Only for non-static transactions
func (tm *TransactionManager) startTxnForRemote(txnPartitions *ongoingTxn, toContact []bool) {
	indexesToWait := make([]int, 0, len(toContact))

	for i, hasTo := range toContact {
		if hasTo && txnPartitions.txnDataToUse[i] == nil {
			/*
					conn, err := net.Dial("tcp", tm.remoteIPs[i])
					utilities.CheckErr("Network connection establishment err on remote read", err)
					txnPartitions.conns[i] = conn
				SendProto(StartTrans, CreateStartTransaction(txnPartitions.originalClk.ToBytes()), conn)
			*/
			//SendProto(StartTrans, CreateStartTransaction(txnPartitions.originalClk.ToBytes()), txnPartitions.conns[i])
			//txnPartitions.replyChans[i], txnPartitions.lockChans[i] = tm.connPool.sendAndLockRequest(StartTrans, CreateStartTransaction(txnPartitions.originalClk.ToBytes()), i)
			//txnPartitions.replyChans[i] = tm.connPool.sendAndLockRequest(StartTrans, CreateStartTransaction(txnPartitions.originalClk.ToBytes()), i)
			txnPartitions.replyChans[i] = tm.connPool.sendAndLockRequest(S2S, CreateS2SWrapperProto(uint64(txnPartitions.TransactionId),
				proto.WrapperType_START_TXN, CreateStartTransaction(txnPartitions.originalClk.ToBytes())), i)
			indexesToWait = append(indexesToWait, i)
			//txnPartitions.nConns++
			txnPartitions.nTxnsStarted++
		}
	}

	for _, index := range indexesToWait {
		reply := <-txnPartitions.replyChans[index]
		txnPartitions.lockChans[index] = reply.lockChan
		//replyProto := reply.msg.(*proto.S2SWrapperReply).StartTxn
		//_, replyProto, _ := ReceiveProto(txnPartitions.conns[index])
		//txnPartitions.txnDataToUse[index] = replyProto.(*proto.ApbStartTransactionResp).GetTransactionDescriptor()
		txnPartitions.txnDataToUse[index] = reply.msg.StartTxn.GetTransactionDescriptor()
	}

}

// Note: Done by a separate goroutine
func (tm *TransactionManager) handleRemoteReads(txnPartitions *ongoingTxn, reqsPerServer [][]crdt.ReadObjectParams,
	remoteReqsToChan [][]int, readChan chan tools.Pair[int, crdt.State]) {

	//if txnPartitions.nConns < len(reqsPerServer) {
	if txnPartitions.nTxnsStarted < len(reqsPerServer) {
		toContact, has := make([]bool, len(reqsPerServer)), false
		for i, reqs := range reqsPerServer {
			if len(reqs) > 0 {
				toContact[i], has = true, true
			}
		}
		if has {
			tm.startTxnForRemote(txnPartitions, toContact)
		}
	}

	for i, reqs := range reqsPerServer {
		if len(reqs) > 0 {
			//SendProto(ReadObjs, CreateReadObjs(txnPartitions.txnDataToUse[i], reqs), txnPartitions.conns[i])
			//txnPartitions.lockChans[i] <- msgToSend{code: ReadObjs, needsLock: true, msg: CreateReadObjs(txnPartitions.txnDataToUse[i], reqs), replyChan: txnPartitions.replyChans[i]}
			txnPartitions.lockChans[i] <- msgToSend{code: S2S, needsLock: true, msg: CreateS2SWrapperProto(uint64(txnPartitions.TransactionId),
				proto.WrapperType_READ, CreateRead(txnPartitions.txnDataToUse[i], nil, reqs)), replyChan: txnPartitions.replyChans[i]}
		}
	}

	var readParams crdt.ReadObjectParams
	var currReqs []crdt.ReadObjectParams
	var currIndexes []int
	//Receiving replies and redirecting to state
	//for i, conn := range txnPartitions.conns {
	for i, replyChan := range txnPartitions.replyChans {
		currReqs, currIndexes = reqsPerServer[i], remoteReqsToChan[i]
		if len(currReqs) > 0 {
			//_, protobuf, _ := ReceiveProto(conn)
			//readReply := protobuf.(*proto.ApbReadObjectsResp).GetObjects()
			reply := <-replyChan
			//readReply := reply.msg.(*proto.ApbReadObjectsResp).GetObjects()
			readReply := reply.msg.ReadObjs.GetObjects()
			for j, obj := range readReply {
				readParams = currReqs[j]
				//readChans[currIndexes[j]] <- crdt.ReadRespProtoToAntidoteState(obj, readParams.CrdtType, readParams.ReadArgs.GetREADType())
				readChan <- tools.Pair[int, crdt.State]{First: currIndexes[j], Second: crdt.ReadRespProtoToAntidoteState(obj, readParams.CrdtType, readParams.ReadArgs.GetREADType())}
			}
		}
	}
}

func (tm *TransactionManager) handleRemoteStaticSingleRead(clientID int, readArgs crdt.ReadObjectParams, remoteIndex int, replyChan chan StateClockPair) {
	poolChan := tm.connPool.sendRequest(S2S, CreateS2SWrapperProto(uint64(clientID), proto.WrapperType_STATIC_SINGLE_READ, CreateS2SSingleRead(readArgs)), remoteIndex)
	reply := <-poolChan
	state := crdt.ReadRespProtoToAntidoteState(reply.msg.SingleRead.Resp, readArgs.CrdtType, readArgs.ReadArgs.GetREADType())
	replyChan <- StateClockPair{State: state, Timestamp: clocksi.SliceTimestamp{}.FromBytes(reply.msg.SingleRead.Clk)}
}

// func (tm *TransactionManager) handleRemoteStaticReads(txnID TransactionId, ts clocksi.Timestamp, bufs *TMClientBuffers) {
func (tm *TransactionManager) handleRemoteStaticReads(clientID int, ts clocksi.Timestamp, reqsPerServer [][]crdt.ReadObjectParams,
	remoteReqsToChan [][]int, readChan chan tools.Pair[int, crdt.State]) {

	//conns := make([]net.Conn, len(reqsPerServer))
	//poolChans := make([]chan msgReply, len(bufs.reqsPerServer))
	poolChans := make([]chan msgReply, len(reqsPerServer))
	//Sending reqs
	//for i, reqs := range bufs.reqsPerServer {
	txnIDBytes := createTxnDescriptorBytes(0, ts)
	for i, reqs := range reqsPerServer {
		if len(reqs) > 0 {
			/*
				conn, err := net.Dial("tcp", tm.remoteIPs[i])
				utilities.CheckErr("Network connection establishment err on remote read", err)
				conns[i] = conn
				SendProto(StaticReadObjs, CreateStaticReadObjs(ts.ToBytes(), reqs), conn)
			*/
			//SendProto(StaticReadObjs, CreateStaticReadObjs(ts.ToBytes(), reqs), ongoingRemote.conns[i])
			poolChans[i] = tm.connPool.sendRequest(S2S, CreateS2SWrapperProto(uint64(clientID),
				proto.WrapperType_STATIC_READ, CreateStaticRead(txnIDBytes, nil, reqs)), i)
		}
	}

	var readParams crdt.ReadObjectParams
	var currReqs []crdt.ReadObjectParams
	var currIndexes []int
	//Receiving replies and redirecting to state
	//for i, conn := range conns {
	for i, poolChan := range poolChans {
		//currReqs, currIndexes = bufs.reqsPerServer[i], bufs.remoteReqsToChan[i]
		currReqs, currIndexes = reqsPerServer[i], remoteReqsToChan[i]
		if len(currReqs) > 0 {
			//fmt.Println("[TM][StaticReadRemote]Waiting on channel with ID", int32(txnID))
			msgReply := <-poolChan
			//fmt.Println("[TM][StaticReadRemote]Got reply from channel with ID", int32(txnID))
			//_, protobuf, _ := ReceiveProto(conn)
			protobuf := msgReply.msg
			//readReply := protobuf.(*proto.ApbStaticReadObjectsResp).GetObjects().GetObjects()
			readReply := protobuf.StaticReadObjs.GetObjects().GetObjects()
			//fmt.Printf("[TM][S2S]Got reply from server %d with objects %+v\n", i, readReply)
			for j, obj := range readReply {
				readParams = currReqs[j]
				//fmt.Printf("[TM][S2S]ReadParams: %+v\n", readParams)
				//readChans[currIndexes[j]] <- crdt.ReadRespProtoToAntidoteState(obj, readParams.CrdtType, readParams.ReadArgs.GetREADType())
				readChan <- tools.Pair[int, crdt.State]{First: currIndexes[j], Second: crdt.ReadRespProtoToAntidoteState(obj, readParams.CrdtType, readParams.ReadArgs.GetREADType())}
				//bufs.readChans[currIndexes[j]] <- crdt.ReadRespProtoToAntidoteState(obj, readParams.CrdtType, readParams.ReadArgs.GetREADType())
			}
			//bufs.reqsPerServer[i], bufs.remoteReqsToChan[i] = bufs.reqsPerServer[i][:0], bufs.remoteReqsToChan[i][:0]
			reqsPerServer[i], remoteReqsToChan[i] = reqsPerServer[i][:0], remoteReqsToChan[i][:0]
		}
	}
	//fmt.Println("[TM][StaticReadRemote]Got reply from all channels")
}

func (tm *TransactionManager) handleRemoteUpds(txnPartitions *ongoingTxn, reqsPerServer []tools.SliceWithCounter[crdt.UpdateObjectParams]) {

	//if txnPartitions.nConns < len(reqsPerServer) {
	if txnPartitions.nTxnsStarted < len(reqsPerServer) {
		toContact, has := make([]bool, len(reqsPerServer)), false
		for i, reqs := range reqsPerServer {
			if reqs.Len() > 0 {
				toContact[i], has = true, true
			}
		}
		if has {
			tm.startTxnForRemote(txnPartitions, toContact)
		}
	}

	//Sending reqs
	for i, reqs := range reqsPerServer {
		if reqs.Len() > 0 {
			//SendProto(UpdateObjs, CreateUpdateObjs(txnPartitions.txnDataToUse[i], reqs), txnPartitions.conns[i])
			//txnPartitions.lockChans[i] <- msgToSend{code: UpdateObjs, needsLock: true, msg: CreateUpdateObjs(txnPartitions.txnDataToUse[i], reqs), replyChan: txnPartitions.replyChans[i]}
			txnPartitions.lockChans[i] <- msgToSend{code: S2S, needsLock: true, msg: CreateS2SWrapperProto(uint64(txnPartitions.TransactionId),
				proto.WrapperType_UPD, CreateUpdateObjs(txnPartitions.txnDataToUse[i], reqs.ToSlice())), replyChan: txnPartitions.replyChans[i]}
		}
	}

	var currReqs tools.SliceWithCounter[crdt.UpdateObjectParams]
	//for i, conn := range txnPartitions.conns {
	for i, replyChan := range txnPartitions.replyChans {
		currReqs = reqsPerServer[i]
		if currReqs.Len() > 0 {
			//ReceiveProto(conn) //Waits until the other server acks the write
			<-replyChan
		}
	}
}

// Note: done by a separate goroutine
func (tm *TransactionManager) handleRemoteCommit(txnPartitions *ongoingTxn, remoteChan chan bool) {
	//for i, conn := range txnPartitions.conns {
	for i, lockChan := range txnPartitions.lockChans {
		//if conn != nil {
		if txnPartitions.txnDataToUse[i] != nil {
			//SendProto(CommitTrans, CreateCommitTransaction(txnPartitions.txnDataToUse[i]), conn)
			//lockChan <- msgToSend{code: CommitTrans, needsLock: false, msg: CreateCommitTransaction(txnPartitions.txnDataToUse[i]), replyChan: txnPartitions.replyChans[i]}
			lockChan <- msgToSend{code: S2S, needsLock: false, msg: CreateS2SWrapperProto(uint64(txnPartitions.TransactionId),
				proto.WrapperType_COMMIT, CreateCommitTransaction(txnPartitions.txnDataToUse[i])), replyChan: txnPartitions.replyChans[i]}
			//close(lockChan)
		}
	}

	//for i, conn := range txnPartitions.conns {
	for i, replyChan := range txnPartitions.replyChans {
		//if conn != nil {
		if txnPartitions.txnDataToUse[i] != nil {
			//ReceiveProto(conn) //Waits until the other server acks the commit
			//conn.Close()
			<-replyChan
		}
	}

	remoteChan <- true
}

func (tm *TransactionManager) handleRemoteAbort(txnPartitions *ongoingTxn, remoteChan chan bool) {
	//for i, conn := range txnPartitions.conns {
	for i, lockChan := range txnPartitions.lockChans {
		//if conn != nil {
		if txnPartitions.txnDataToUse[i] != nil {
			//SendProto(AbortTrans, CreateAbortTransaction(txnPartitions.txnDataToUse[i]), conn)
			//lockChan <- msgToSend{code: AbortTrans, needsLock: false, msg: CreateAbortTransaction(txnPartitions.txnDataToUse[i]), replyChan: txnPartitions.replyChans[i]}
			lockChan <- msgToSend{code: S2S, needsLock: false, msg: CreateS2SWrapperProto(uint64(txnPartitions.TransactionId),
				proto.WrapperType_ABORT, CreateAbortTransaction(txnPartitions.txnDataToUse[i])), replyChan: txnPartitions.replyChans[i]}
			//close(lockChan)
		}
	}

	//for i, conn := range txnPartitions.conns {
	for i, replyChan := range txnPartitions.replyChans {
		//if conn != nil {
		if txnPartitions.txnDataToUse[i] != nil {
			//ReceiveProto(conn) //Waits until the other server acks the abort
			//conn.Close()
			<-replyChan
		}
	}
	remoteChan <- true
}

func (tm *TransactionManager) handleRemoteStaticUpds(txnID TransactionId, ts clocksi.Timestamp, reqsPerServer []tools.SliceWithCounter[crdt.UpdateObjectParams]) {
	//Any data that comes through here is safe for the whole process, as the client blocks until there's a reply.
	//conns := make([]net.Conn, len(reqsPerServer))
	poolChans := make([]chan msgReply, len(reqsPerServer))
	/*var sb strings.Builder
	sb.WriteString("[TM][handleRemoteStaticUpds]Upds for remote. TxnID: ")
	sb.WriteString(strconv.FormatUint(uint64(txnID), 10))
	sb.WriteString(". ")*/
	//Sending reqs
	//fmt.Printf("[TM][handleRemoteStaticUpds]Start txnID %d.\n", txnID)

	/*debugChan := make(chan int, 10)
	go func(dChan chan int, tID TransactionId) {
		origToWait, toWait := 0, 0
		for {
			select {
			case n := <-dChan: //Count.
				toWait, origToWait = n, n
				for i := 0; i < toWait; i++ {
					select {
					case <-dChan: //Replies.
						toWait--
					case <-time.After(12 * time.Second):
						fmt.Printf("[TM][handleRemoteStaticUpds]Timeout waiting for replies for remote txn %d. Received %d replies, expected %d.\n",
							tID, origToWait-toWait, origToWait)
						time.Sleep(8000 * time.Millisecond)
						fmt.Printf("[TM][handleRemoteStaticUpds]Exitting due to replies timeouts.\n")
						os.Exit(1)
					}
				}
				select {
				case _ = <-dChan: //100, all OK.
					return
				case <-time.After(14 * time.Second):
					fmt.Printf("[TM][handleRemoteStaticUpds]Timeout waiting for final confirmation of remote txn %d. Received %d replies out of %d.\n",
						tID, origToWait-toWait, origToWait)
					time.Sleep(2000 * time.Millisecond)
					os.Exit(1)
				}
			case <-time.After(15 * time.Second):
				fmt.Printf("[TM][handleRemoteStaticUpds]Timeout waiting for count of replies for remote txn %d, clk %s\n",
					tID, ts.ToString())
				time.Sleep(2000 * time.Millisecond)
				fmt.Printf("[TM][handleRemoteStaticUpds]Exitting due to count timeouts.\n")
				os.Exit(1)
			}
		}
	}(debugChan, txnID)
	nCount := 0 //TODO: Remove, tmp (as well as debugChan).*/

	for i, reqs := range reqsPerServer {
		if reqs.Len() > 0 {
			/*copy := tools.NewSliceWithCounter[crdt.UpdateObjectParams](reqs.Len()) //TODO: UNDO.
			sb.WriteString(fmt.Sprintf("Server %d: [", i))
			for j := 0; j < reqs.Len(); j++ {
				copy.AddToEnd(reqs.Get(j))
				keyP := reqs.Get(j)
				sb.WriteString(fmt.Sprintf("Key: (%s, %s, %s); ", keyP.KeyParams.Key, keyP.KeyParams.CrdtType, keyP.KeyParams.Bucket))
			}
			sb.WriteString("] ")*/
			//Debugging: timestamp is OK until here, even if we convert to bytes and from bytes.
			poolChans[i] = tm.connPool.sendRequest(S2S, CreateS2SWrapperProto(uint64(txnID),
				proto.WrapperType_STATIC_UPDATE, CreateStaticUpdateObjs(createTxnDescriptorBytes(txnID, ts), reqs.ToSlice())), i) //TODO: Undo, change to reqs.
			//nCount++
		}
	}
	//debugChan <- nCount
	//fmt.Println(sb.String())

	var currReqs tools.SliceWithCounter[crdt.UpdateObjectParams]
	for i, poolChan := range poolChans {
		currReqs = reqsPerServer[i]
		if currReqs.Len() > 0 {
			<-poolChan //Waits until the other server acks the write
			//debugChan <- -1
		}
	}
	//debugChan <- 100
	//fmt.Printf("[TM][handleRemoteStaticUpds]Got all replies for remote txn %d\n", txnID)
}

// The idea of this routine is to allow re-using IDs of clients that closed connection
// as this allows for more efficient solutions.
func (tm *TransactionManager) generateTMIDs() {
	currMaxAvailableID := 0
	reusableIDs := make([]int, 0, 10)
	//Idea: Keep a bunch of IDs already pre-available, fill more as it empties
	for {
		if len(reusableIDs) > 0 { //In this case we do not increment maxID as we are re-using an ID
			tm.newIDChan <- TMClientID((1 << 63) | uint64(reusableIDs[len(reusableIDs)-1])) //Set highest bit to 1 to indicate that it's a reused ID.
			reusableIDs = reusableIDs[:len(reusableIDs)-1]
		} else {
			currMaxAvailableID++
			//TODO: Probably lock this.
			if currMaxAvailableID == len(tm.clksInUse) {
				tm.clksInUse = append(tm.clksInUse, nil)
			}
			tm.newIDChan <- TMClientID(uint64(currMaxAvailableID)) //Highest bit is 0, as it is a new ID
		}
		//Check if any client closed. If so, keep reading until empty
		for len(tm.canReuseIDChan) > 0 {
			reusableIDs = append(reusableIDs, <-tm.canReuseIDChan)
		}
		//Known shortcoming: if at some point there are a lot of clients, and then few, the size of clksInUse will keep being big
		//and thus PotionDB's GC will always check a lot of (nil) positions
	}
}

// We should think if txnToClock and txnWaitFor should be SliceMaps instead.
// Note that Go Map's will re-use deleted slots, so this map won't grow forever (pfew).
// I may want to merge more clocks, and also test if this is a updating bottleneck. E.g., can check the len to see if it's full (possibly can give a very long len)
// Maybe a solution for RemoteTxns clock updating being behind is for the replication routine to keep a local copy of the clk, that is always updated.
// We can do a fast read from TM's clock and merge into that copy, and use that for decision making + sending to the materializer.
// We would have to read from TM's clock anyway, so this seems wise.
// We leverage on the fact that local txns only increase the local replica's clock entry, and that Replicator only increase another remote replica's clk
// (as for a remote txn to be applied, all entries aside from that replica must be >= than the clk received)
// Thus, this allows us to always only update a single pos, avoiding more complex clk merging.
// Note that TM's clock still has to be implemented with the swapping buffers technique, as otherwise all entries in a VC would have to be atomic, and we'd have to atomically read them and copy (and this is dangerous, individual atomics are not globally atomic!)
// This method includes a lot of code repetition, as speed execution is key, thus we want to avoid the overhead of calling auxiliary functions.
func (tm *TransactionManager) handleCommitReplies() {
	if runtime.NumCPU() >= 12 { //No point locking a thread here if we're running on a modest system.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	//Can't replace this with SliceMap as, as of atm, it can easily reach 500+ entries (spike 1000+).
	//Even the average is usually around 50~80, which is too high to be effective for SliceMap.
	//The only workaround would be for TransactionIds to be incremental, with atomic, and use a slice.
	txnToClk := make(map[TransactionId]tools.Pair[clocksi.Timestamp, *int], 200) //Size is somewhat arbitrary, but this map will easily hold several entries.
	waitingTMs := tools.NewHeap(TMWaitClockLess, 100)

	stackedClk := (*tm.localClock.readPtr.Load()).Copy()
	nStacked, ourReplicaID := 0, shared.SortedReplicaID

	commitChan := tm.commitChan

	for {
		switch typedReq := (<-commitChan).(type) {
		//req, _ := commitChan.Read()
		//switch typedReq := req.(type) {
		case TMCommitNPartitions:
			txnToClk[typedReq.txnId] = tools.Pair[clocksi.Timestamp, *int]{First: typedReq.clk, Second: &typedReq.nPartitions}
			tm.localTxnsProc++
			tm.anyUpdatesSinceGC = true
		case TMPartCommitReply:
			pair := txnToClk[typedReq.txnId]
			*pair.Second--
			if *pair.Second == 0 {
				delete(txnToClk, typedReq.txnId)
				stackedClk.UpdatePos(ourReplicaID, pair.First.GetPos(ourReplicaID))
				nStacked++
				if waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) { //If there's waitingTMs that can be answered with this, we update the clock right away and reply to them.
					tm.localClock.UpdatePos(ourReplicaID, stackedClk.GetPos(ourReplicaID))
					copyClk := stackedClk.Copy()
					waitingTMs.Pop().replyChan <- copyClk
					for waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
						waitingTMs.Pop().replyChan <- copyClk
					}
					nStacked = 0
				}
			}
		case TMWaitClock:
			//Possible data-race: when we tried to read/start txn, this clk was not satisfied yet. But in the meantime, it got satisfied. We must check it as, if updates cease, we will never reply to this client.
			if typedReq.targetClk.IsLowerOrEqual(stackedClk) {
				if nStacked > 0 { //Update clock.
					tm.localClock.UpdatePos(ourReplicaID, stackedClk.GetPos(ourReplicaID))
					nStacked = 0
				}
				copyClk := stackedClk.Copy()
				typedReq.replyChan <- copyClk //No need to check waitingTMs, as all of those there are, for sure, > stackedClk.
			} else { //Most likely scenario.
				waitingTMs.Push(typedReq)
			}
		/*case TMCommitReplTxn: //Update clock right away.
		sortedReplicaID := clocksi.GetSortedPosOfId(typedReq.replicaID)
		stackedClk.UpdatePos(sortedReplicaID, typedReq.Clk.GetPos(sortedReplicaID))
		if nStacked > 0 { //We need to update two positions.
			tm.localClock.UpdateTwoPos(sortedReplicaID, stackedClk.GetPos(sortedReplicaID), ourReplicaID, stackedClk.GetPos(ourReplicaID))
			nStacked = 0
		} else { //Update only remote pos
			tm.localClock.UpdatePos(sortedReplicaID, stackedClk.GetPos(sortedReplicaID))
		}
		if waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
			copyClk := stackedClk.Copy()
			waitingTMs.Pop().replyChan <- copyClk
			for waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
				waitingTMs.Pop().replyChan <- copyClk
			}
		}*/
		case TMCommitReplClk: //Update clock right away. Note: replicaID is already the sorted one.
			sortedReplicaID := typedReq.sortedReplicaID
			stackedClk.UpdatePos(sortedReplicaID, typedReq.stableTs)
			if nStacked > 0 { //We need to update two positions.
				tm.localClock.UpdateTwoPos(sortedReplicaID, stackedClk.GetPos(sortedReplicaID), ourReplicaID, stackedClk.GetPos(ourReplicaID))
				nStacked = 0
			} else { //Update only remote pos
				tm.localClock.UpdatePos(sortedReplicaID, stackedClk.GetPos(sortedReplicaID))
			}
			if waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
				copyClk := stackedClk.Copy()
				waitingTMs.Pop().replyChan <- copyClk
				for waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
					waitingTMs.Pop().replyChan <- copyClk
				}
			}
			tm.anyUpdatesSinceGC = true
		case TMCommitReplFullClk: //Update clock right away.
			if nStacked > 0 {
				stackedClk.MergeInto(typedReq.clk) //We can't do the other way around, as typedReq.clk is shared with partitions.
				nStacked = 0
				tm.localClock.Update(stackedClk)
			} else {
				tm.localClock.Update(typedReq.clk)
			}
			if waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
				copyClk := stackedClk.Copy()
				waitingTMs.Pop().replyChan <- copyClk
				for waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
					waitingTMs.Pop().replyChan <- copyClk
				}
			}
			tm.anyUpdatesSinceGC = true
		case TMCommitReplClkOnly: //Update clock right away. There may or may not have been any updates associated to this clock.
			sortedReplicaID := typedReq.sortedReplicaID
			stackedClk.UpdatePos(sortedReplicaID, typedReq.stableTs)
			if nStacked > 0 { //We need to update two positions.
				tm.localClock.UpdateTwoPos(sortedReplicaID, stackedClk.GetPos(sortedReplicaID), ourReplicaID, stackedClk.GetPos(ourReplicaID))
				nStacked = 0
			} else { //Update only remote pos
				tm.localClock.UpdatePos(sortedReplicaID, stackedClk.GetPos(sortedReplicaID))
			}
			if waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
				copyClk := stackedClk.Copy()
				waitingTMs.Pop().replyChan <- copyClk
				for waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
					waitingTMs.Pop().replyChan <- copyClk
				}
			}
		}
		if nStacked > 0 { //Depending on certain conditions, we may update the localClock early.
			reqsLeft := len(tm.commitChan)
			//reqsLeft := commitChan.Size()
			if reqsLeft == 0 || nStacked >= 100 || (nStacked >= 5 && len(tm.commitChan) < int(nGoRoutines/4)) {
				//if reqsLeft == 0 || nStacked >= 100 || (nStacked >= 5 && commitChan.Size() < uint32(nGoRoutines/4)) {
				//Idea: if no requests left -> always update localClock. If many already stacked, force an upd for next reads to have a recent clk. If a few stacked, and few reqs left, the chance of being able to stack more are slim -> update now.
				tm.localClock.UpdatePos(ourReplicaID, stackedClk.GetPos(ourReplicaID))
				nStacked = 0
				if waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
					copyClk := stackedClk.Copy()
					waitingTMs.Pop().replyChan <- copyClk
					for waitingTMs.Len() > 0 && waitingTMs.PeekMin().targetClk.IsLowerOrEqual(stackedClk) {
						waitingTMs.Pop().replyChan <- copyClk
					}
				}
			}
		}
		//if len(tm.commitChan) >= cap(tm.commitChan)-1 {
		/*if commitChan.Size() >= 1023 {
			//TODO: Remove this.
			//Print a warning message if the commit chan is or was full when we pulled this request.
			//fmt.Printf("[TM][HandleCommitReplies]Routine handling clk updates is overloaded! Chan is/was full. Len/cap: %d/%d. Current time: %s\n", len(tm.commitChan), cap(tm.commitChan), time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("[TM][HandleCommitReplies]Routine handling clk updates is overloaded! Chan is/was full. Len/cap: %d/%d. Current time: %s\n", commitChan.Size(), 2048, time.Now().Format("2006-01-02 15:04:05.000"))
		}*/
	}
}

// Note: This may return 0 if called before TM is fully initialized.
func (tm *TransactionManager) GetClkByteSize() int {
	return tm.localClock.GetClock().GetBytesSize()
}

//Debug

func (tm *TransactionManager) sanityCheck() {
	for {
		time.Sleep(50 * time.Second)
		fmt.Println("[TM][SC]Clock: ", tm.localClock.GetClock().ToSortedString())
		//fmt.Println("[TM][SC]Clock: ", tm.localClock.SliceTimestamp.ToSortedString())
		for id, msgs := range tm.downstreamQueue {
			if !msgs.IsEmpty() {
				fmt.Println("[TM][SC]There's still leftover msgs in downstreamQueue! ID:", id, msgs.ToSlice())
			}
		}
	}
}
