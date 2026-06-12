package components

import (
	fmt "fmt"
	"math"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"potionDB/potionDB/utilities"
	"potionDB/shared/shared"
	"sync/atomic"
	"time"

	"github.com/AndreRijo/go-tools/src/tools"
)

type Replicator struct {
	tm               *TransactionManager //to send request to downstream transactions
	localPartitions  []Logger
	currTxnCache     []tools.SliceWithHideable[PairClockUpdates]   //First index: partitionID. Contains the oldest slice of txns obtained from each log.
	overflowTxnCache []tools.SliceWithHideable[[]PairClockUpdates] //Contains the non-oldest slices of txns obtained from each log.
	lastLogClk       []clocksi.Timestamp                           //The last received stable clk from each partition, in order to know what clock to request from each partition.
	//maxCommonClk     clocksi.Timestamp                             //The highest clk common to all partitions.
	maxCommonTs int64 //Max clock value of this replica that is common to all partitions.
	remote      *RemoteGroup
	created     bool //True if Replicator instance has already been initialized.
	replicaID   uint16
	buckets     []string
	JoinInfo
	allReplicaIDs []uint16 //Stores the replicaIDs of all replicas
	partsChan     chan StableClkUpdatesPair
	allPartsDone  bool //Set to true when currTxnCache and overflowTxnCache are fully empty. It's set to false as soon as a txn comes from the Logs.

	//New things added
	partUpdsBuf        []tools.SliceWithCounter[crdt.UpdateObjectParams]                                  //Re-usable buffer that holds upds (of a txn/merged txn) per partition.
	logBuffersToReturn tools.SliceWithCounter[tools.Pair[int, tools.SliceWithHideable[PairClockUpdates]]] //Buffers to return to the logger, when it is convenient.
	replicationStarted atomic.Bool                                                                        //True if doReplication() has already been called                                                                //Set this to true to speed up replication of initial data loading.
	//Protocol: 0 means false, 2 means it got requested. 1 means we already got informed by TPC-H dataload that all data has been sent to the loggers.
	//This way, even if TPC-H dataload finishes before we even start Replication (due to RabbitMQ taking long to start), we will still know that we need to do initial data replication.
	initialDataFastRepl atomic.Int64
}

type JoinInfo struct {
	holdMsgs       []ReplicatorMsg
	waitFor        int
	holdReplyJoins []*ReplyJoin
	nHoldJoins     int
	allDone        bool
}

type ReplicatorMsg interface {
	getSenderID() uint16
}

/*
type ReplicatorTxn struct {
	Clk      clocksi.Timestamp
	Upds     map[int64][]crdt.UpdateObjectParams
	SenderID uint16
	TxnID    int32 //This is filled by remoteConnection.go. When receiving txns, this is used to identify when a different txn is being received.
}

type ReplicatorGroupTxn struct {
	Txns     []RemoteTxnSlice
	SenderID uint16
	TxnID    int32 //This is filled by remoteConnection.go. When receiving txns, this is used to identify when a different txn is being received.
}

type RemoteTxnSlice struct {
	Txns []RemoteTxn
	clocksi.Timestamp
}*/

/*
type ReplicatorRequest struct {
	clocksi.Timestamp
	Upds        []crdt.UpdateObjectParams
	SenderID    uint16
	PartitionID int64
	TxnID       int32 //This is filled by remoteConnection.go. When receiving txns, this is used to identify when a different txn is being received.
}*/

type StableClock struct {
	SenderID uint16
	Ts       int64
}

type RemoteTxn struct {
	SenderID uint16
	Clk      clocksi.Timestamp
	Upds     map[int][]crdt.UpdateObjectParams
	TxnID    int32 //This is filled by remoteConnection.go. Used to identify the transactions
}

/*type RemoteTxnGroup struct {
	SenderID uint16
	Txns     []RemoteTxn
	MinTxnID int32 //This is filled by remoteConnection.go. Used to identify the transactions
	MaxTxnID int32 //This is filled by remoteConnection.go. Used to identify the transactions
}*/

type RemoteID struct {
	SenderID uint16
	Buckets  []string
	IP       string
}

type RemoteTrigger struct {
	AutoUpdate
	IsGeneric bool
}

// All join-related requests include a field to identify in the remoteGroup which connection the reply should be sent to.
type Join struct {
	SenderID   uint16
	ReplyID    uint16
	CommonBkts []string
	ReqIP      string
}

type ReplyJoin struct {
	SenderID   uint16
	ReplyID    uint16
	Clks       []clocksi.Timestamp
	CommonBkts []string
	ReqIP      string
}

type RequestBucket struct {
	SenderID uint16
	ReplyID  uint16
	Buckets  []string
	ReqIP    string
}

type ReplyBucket struct {
	SenderID   uint16
	PartStates [][]*proto.ProtoCRDT
	Clk        clocksi.Timestamp
}

type ReplyEmpty struct{}

const (
	//TS_SEND_DELAY time.Duration = 2000 //milliseconds
	TS_SEND_DELAY time.Duration = 5000
	//TS_SEND_DELAY time.Duration = 20000
	//TS_SEND_DELAY         time.Duration = 500
	cacheInitialSize    = 100
	toSendInitialSize   = 10
	joinHoldInitialSize = 100
	DO_JOIN             = "doJoin"
	//REPL_MAX_TXN_MERGE  = 1000
	REPL_MAX_TXN_MERGE         = 5000
	OVERFLOW_INITIAL_SIZE      = 3
	PART_UPDS_BUF_INITIAL_SIZE = 100
	//when the amount of time spent preparing txns to be remote is very short, it is assumed that PotionDB is under low load.
	//If the variable below is true, at those times we'll perform the next replication much sooner.
	FAST_REPL_WHEN_LOW_LOAD                         = true
	INITIAL_DATA_REPL_CHECK_FREQUENCY time.Duration = 100 //(ms). While PotionDB is loading initial data, if initialDataFastRepl is true, the replicator will check for new txns with this frequency.
)

var (
	doesJoin        bool
	localPotionIP   string
	localRabbitMQIP string
)

/*
func (req ReplicatorRequest) getSenderID() uint16 {
	return req.SenderID
}

func (req ReplicatorTxn) getSenderID() uint16 {
	return req.SenderID
}

func (req ReplicatorGroupTxn) getSenderID() uint16 {
	return req.SenderID
}*/

func (req StableClock) getSenderID() uint16 {
	return req.SenderID
}

func (req RemoteTxn) getSenderID() uint16 {
	return req.SenderID
}

/*func (req RemoteTxnGroup) getSenderID() uint16 {
	return req.SenderID
}*/

func (req RemoteID) getSenderID() uint16 {
	return req.SenderID
}

// Sender ID is irrelevant. It's just here to implement the ReplicatorMsg interface.
func (req RemoteTrigger) getSenderID() uint16 {
	return 0
}

func (req Join) getSenderID() uint16 {
	return req.SenderID
}

func (req ReplyJoin) getSenderID() uint16 {
	return req.SenderID
}

func (req RequestBucket) getSenderID() uint16 {
	return req.SenderID
}

func (req ReplyBucket) getSenderID() uint16 {
	return req.SenderID
}

func (req ReplyEmpty) getSenderID() uint16 {
	return 0
}

func (repl *Replicator) DisableInitialDataloadMode() {
	repl.initialDataFastRepl.Add(-1)
	fmt.Printf("[REPL]Requested disabling of initial data loading replication mode at %s.\n", time.Now().Format("15:04:05.000"))
}

//TODO: Some GC mechanism to reduce the size of the buffers?

func (repl *Replicator) Reset() {
	if !shared.IsReplDisabled {
		for id := 0; id < int(nGoRoutines); id++ {
			repl.lastLogClk[id] = clocksi.MinimumTs
			if repl.currTxnCache[id].Len() > 0 {
				repl.currTxnCache[id] = tools.SliceWithHideable[PairClockUpdates]{} //Reset.
			}
			if repl.overflowTxnCache[id].Len() > 0 {
				repl.overflowTxnCache[id].DeepClear()
			}
			if repl.partUpdsBuf[id].Cap() == PART_UPDS_BUF_INITIAL_SIZE {
				if repl.partUpdsBuf[id].Len() > 0 {
					repl.partUpdsBuf[id].DeepClear()
				}
			} else {
				repl.partUpdsBuf[id] = tools.NewSliceWithCounter[crdt.UpdateObjectParams](PART_UPDS_BUF_INITIAL_SIZE)
			}
		}
		fmt.Printf("[REPL]Reset complete at %s.\n", time.Now().Format("15:04:05.000"))
	}
}

func (repl *Replicator) Initialize(tm *TransactionManager, loggers []Logger, buckets []string, replicaID uint16, initialDataLoad bool) {
	if !shared.IsReplDisabled {
		if !repl.created {
			repl.tm = tm
			repl.created = true
			repl.localPartitions = loggers
			if initialDataLoad {
				repl.initialDataFastRepl.Add(2) //Set to 2 to indicate that initial data loading is requested.
			}
			bucketsToListen := buckets

			remoteConn := CreateRemoteGroupStruct(bucketsToListen, replicaID)
			repl.remote = remoteConn
			repl.buckets = bucketsToListen
			repl.replicaID = replicaID
			repl.partsChan = make(chan StableClkUpdatesPair, nGoRoutines)
			repl.currTxnCache, repl.overflowTxnCache = make([]tools.SliceWithHideable[PairClockUpdates], nGoRoutines), make([]tools.SliceWithHideable[[]PairClockUpdates], nGoRoutines)
			repl.lastLogClk, repl.partUpdsBuf = make([]clocksi.Timestamp, nGoRoutines), make([]tools.SliceWithCounter[crdt.UpdateObjectParams], nGoRoutines)
			for i := 0; i < int(nGoRoutines); i++ {
				repl.overflowTxnCache[i] = tools.NewSliceWithHideable[[]PairClockUpdates](OVERFLOW_INITIAL_SIZE)
				repl.lastLogClk[i] = clocksi.MinimumTs
				repl.partUpdsBuf[i] = tools.NewSliceWithCounter[crdt.UpdateObjectParams](PART_UPDS_BUF_INITIAL_SIZE)
			}
			repl.logBuffersToReturn = tools.NewSliceWithCounter[tools.Pair[int, tools.SliceWithHideable[PairClockUpdates]]](int(2 * nGoRoutines))

			//Also skips the joining algorithm if there's no replica to join
			if doesJoin && len(remoteConn.conns) > 0 {
				fmt.Println("[REPL]Join mode")
				repl.JoinInfo = JoinInfo{holdMsgs: make([]ReplicatorMsg, 0, joinHoldInitialSize), allDone: false, nHoldJoins: 0}
				repl.joinGroup()
			} else {
				//Wait for replicaIDs of existing replicas
				repl.JoinInfo.waitFor = int(remoteConn.nReplicas)
				crdt.NReplicas = int32(remoteConn.nReplicas + 1)
				fmt.Printf("[REPL]Not doing join, will wait for replicaID of existing replicas. IDs to receive: %d, number of replicas (including self): %d\n",
					repl.JoinInfo.waitFor, crdt.NReplicas)
				go remoteConn.sendReplicaID(bucketsToListen, localPotionIP)
				go repl.receiveRemoteTxns()
				//go repl.doReplication()
				//If there's no other replica, we can start right away
				if len(remoteConn.conns) == 0 {
					fmt.Println("[REPL] PotionDB in single server mode.")
					repl.allDone = true
					//go repl.replicateCycle()
					ok := repl.replicationStarted.CompareAndSwap(false, true)
					if ok {
						go repl.doReplication()
					}
					//go repl.tm.SendRemoteMsg(TMStart{}) //Different thread to avoid blocking
					repl.tm.SendRemoteMsg(TMStart{}) //OK as channel is non-blocking.
				}
			}
		}
	} else {
		fmt.Println("[REPL] Warning - replicator is disabled. Starting PotionDB in single server mode.")
		tm.SendRemoteMsg(TMStart{})
	}
}

func (repl *Replicator) doReplication() {
	if repl.initialDataFastRepl.Load() > 0 {
		repl.initialDataReplCycle()                  //This will only return once initialDataFastRepl is set to false and an extra cycle is done.
		time.Sleep(TS_SEND_DELAY * time.Millisecond) //Wait a bit before starting the main replication cycle.
	}
	repl.replicateCycle()
}

func (repl *Replicator) initialDataReplCycle() {
	//Do this until initialDataFastRepl gets disabled.
	//Here we don't need maxCommonTs shenanigans, namely no need to choose which txns to replicate - all can be replicated as all have a dummy clk of 0.
	//Also, no stableClk.
	//Note: while we send to the logger the last clk returned by that logger, the logger ignores that clk. He'll simply return all operations in the log.
	done := false
	logRequest := LoggerRequest{LogRequestArgs: LogTxnArgs{lastClock: clocksi.DummyTs.Copy(), ReplyChan: repl.partsChan}}
	//updsCache := make([][]PairClockUpdates, nGoRoutines) //Cache to store updates to send. First index: partitionID.
	var toSleep time.Duration
	emptyInARow := 0
	nReplicated := 0
	dummyClk := clocksi.DummyTs.Copy()
	fmt.Printf("[REPL]Initial data loading replication started at %s. Replication will proceed at a fast pace.\n", time.Now().Format("15:04:05.000"))
	for !done {
		//This works as when initialDataFastRepl gets set to false, we know all operations are already in the logger's channels (or in their slices even).
		//The nReplicated > 0 is to protect against the dataload finishing before we connect to TM.
		if repl.initialDataFastRepl.Load() <= 1 && nReplicated > 0 {
			done = true //Still do this replication cycle though.
		}
		start := time.Now().UnixNano()
		for _, part := range repl.localPartitions {
			part.SendLoggerRequest(logRequest)
		}
		nPartsWithUpds := 0
		for id := 0; id < int(nGoRoutines); id++ {
			upds := (<-repl.partsChan).upds
			if len(upds) > 0 {
				count := 0
				for _, pair := range upds {
					count += len(pair.upds)
				}
				hold := make([]crdt.UpdateObjectParams, count)
				pos := 0
				for _, pair := range upds {
					copy(hold[pos:], pair.upds)
					pos += len(pair.upds)
				}
				repl.remote.SendTxn(RemoteTxn{Clk: dummyClk, Upds: map[int][]crdt.UpdateObjectParams{id: hold}})
				nPartsWithUpds++
				repl.logBuffersToReturn.Append(tools.Pair[int, tools.SliceWithHideable[PairClockUpdates]]{First: id, Second: tools.ToSliceWithHideable(upds)})
			}
		}
		if nPartsWithUpds > 0 {
			/*txn := RemoteTxn{Clk: clocksi.DummyTs.Copy(), Upds: updsMap}
			updsMap = make(map[int][]crdt.UpdateObjectParams, nGoRoutines/2)
			repl.remote.SendTxn(txn)*/
			emptyInARow = 0
			repl.remote.SendStableClk(0) //To signal that the txn(s) have ended.
			repl.returnBuffersToLog()
			nReplicated++
		} else {
			emptyInARow++
		}
		finish := time.Now()
		taken := time.Duration((finish.UnixNano() - start) / 1000000)
		toSleep = INITIAL_DATA_REPL_CHECK_FREQUENCY - taken
		if toSleep > 10 {
			if nPartsWithUpds == 0 && emptyInARow&7 == 1 {
				fmt.Printf("[REPL]Initial data loading - nothing to replicate, sleeping %dms, at %s.\n", toSleep, finish.Format("15:04:05.000"))
			} else if nPartsWithUpds > 0 {
				fmt.Printf("[REPL]Initial data loading - replicated data from %d partitions, took %dms to prepare, sleeping %dms, at %s.\n", nPartsWithUpds, taken, toSleep, finish.Format("15:04:05.000"))
			}
			time.Sleep(toSleep * time.Millisecond)
		} else {
			fmt.Printf("[REPL]Initial data loading - took %d ms, replicated data from %d partitions, will check again right away, at %s.\n", taken, nPartsWithUpds, finish.Format("15:04:05.000"))
		}
	}
	fmt.Printf("[REPL]Exitting initial data loading replication cycle at %s. Will proceed with normal replication cycle now.\n", time.Now().Format("15:04:05.000"))
	repl.initialDataFastRepl.Add(-1)
}

// Implementation note: Replicas recognize that all txns have been sent by receiving a stable clk.
// More precisely, a replica knows it has received a txn fully when it starts receiving the next txn (it identifies a different txnID)
// Thus, for the last txn, it needs to receive a stable clk to recognize the end of that txn.
func (repl *Replicator) replicateCycle() {
	replSinceLastGC := false //Unused for now. In the future, if we need to do some cleaning/GC, we can use this to detect when that might be needed.
	count, start, finish, toSleep, taken := time.Duration(0), int64(0), int64(0), time.Duration(0), time.Duration(0)
	var startFull time.Time
	nTxns, prevNTxns, nTotalTxns, nTotalTxnGroups, nTxnGroups := 0, 0, 0, 0, 0

	dataChan := make(chan tools.Pair[RemoteTxn, int], 50) //Channel to receive data from prepareData. Size is somewhat arbitrary.

	fmt.Printf("[REPL]Replication cycle started at %s. Ready to replicate txns.\n", time.Now().Format("15:04:05.000"))

	for {
		startFull = time.Now()
		start = startFull.UnixNano()

		//fmt.Printf("[REPL]Requesting new txns...\n")
		repl.getNewTxns()
		//fmt.Printf("[REPL]Requesting data preparation...\n")
		if !repl.allPartsDone {
			go repl.prepareData(dataChan)
			count++
			for pair := <-dataChan; pair.Second != 0; pair = <-dataChan { //Keep receiving until prepareData signals that we're done (i.e., return 0)
				//fmt.Printf("[REPL]Got txn/grouped txn from dataChan, with %d merged txns. Total so far: %d. Clock: %s. NParts of txn: %d. Real life time: %s\n", pair.Second, nTxns+pair.Second, pair.First.Clk.ToString(), len(pair.First.Upds), time.Now().Format("15:04:05.000"))
				replSinceLastGC = true
				if pair.Second != 0 {
					repl.remote.SendTxn(pair.First)
					nTxns += pair.Second
					nTxnGroups++
				}
			}
			//Send clock to ensure all replicas receive the latest clock.
			//This is also used to know that the last transaction sent has ended.
			fmt.Printf("[REPL]Got %d txns, sending stableClk of %d (our replicaID: %d).\n", nTxns, repl.maxCommonTs, repl.replicaID)
			repl.remote.SendStableClk(repl.maxCommonTs)
			//repl.remote.SendStableClk(repl.maxCommonClk.GetPos(shared.SortedReplicaID))
			finish = time.Now().UnixNano()
			if nTxns > 0 {
				fmt.Printf("[REPL]Requested sending of %d txns, split across %d groups, at %s. Took %dms preparing.\n", nTxns, nTxnGroups, time.Now().Format("15:04:05.000"), (finish-start)/1000000)
				nTotalTxns += nTxns
				nTotalTxnGroups += nTxnGroups
				repl.returnBuffersToLog()
			} else if nTxns == 0 && replSinceLastGC { //Take the opportunity to do some GC. We would be sleeping this routine anyway.
				fmt.Printf("[REPL]Didn't get any txn, but sent stableClk.\n")
				repl.cleanState()
				replSinceLastGC = false
			}
		} /*else {
			repl.remote.SendStableClk(repl.maxCommonClk.GetPos(shared.SortedReplicaID)) //Keep sending a clk update.
		}*/
		finish = time.Now().UnixNano()
		taken = time.Duration((finish - start) / 1000000)
		toSleep = TS_SEND_DELAY - taken
		if FAST_REPL_WHEN_LOW_LOAD && nTxns > 0 && toSleep > 10 && taken*5 < TS_SEND_DELAY { //Idea: replication was very fast, so PotionDB is likely under light load. Attempt early replication.
			if nTxns < 200 && taken < 5 { //Very fast replication, might be just NuCRDT ops. Very short sleep and send again.
				toSleep = 10
				time.Sleep(10 * time.Millisecond)
			} else {
				toSleep = tools.Max(TS_SEND_DELAY/10, 100)
				fmt.Printf("[REPL]Last replication of %d txns was fast (%dms to prepare), will sleep only for %d ms.\n", nTxns, taken, toSleep)
				time.Sleep(toSleep * time.Millisecond)
			}
		} else if toSleep > 10 {
			time.Sleep(toSleep * time.Millisecond)
		} else {
			fmt.Printf("[REPL]Warning - Replicator might be falling behind! Took %dms to prepare %d txns, but replication frequency is every %dms.\n", taken, nTxns, TS_SEND_DELAY)
		}
		if nTxns == 0 && (prevNTxns > 0 || count*TS_SEND_DELAY%60000 == 0) {
			fmt.Printf("[REPL]No txns to send. Total txns, groups replicated so far: %d, %d\n", nTotalTxns, nTotalTxnGroups)
		}
		prevNTxns = nTxns
		nTxns, nTxnGroups = 0, 0
	}
}

// This is called only when Replicator is idle - that is, no new txns can be sent. So it's a great GC opportunity.
func (repl *Replicator) cleanState() {
	for i := 0; i < len(repl.partUpdsBuf); i++ {
		repl.partUpdsBuf[i].DeepClear()
	}
	//logBuffersToReturn's buffers are always deeply cleaned after each iteration, and returned to the respective logers.
	//currTxnCache's entries are freed during prepareData(), anytime that there's no more updates to replicate.
	//overflowTxnCache's is also deeply clean during prepareData(), anytime that the remaining updates fit in a single buffer. The inner slices are simply freed, so Go's GC will clean them later.
	//So, when Replicator is idle, logBuffersToReturn, currTxnCache and overflowTxnCache are empty and clean.
	//It's a good opportunity to request Logs to clean themselves.
	for _, part := range repl.localPartitions {
		part.SendLoggerRequest(LoggerRequest{LogRequestArgs: LogGCArgs{}})
	}
}

func (repl *Replicator) getNewTxns() {
	//Request for new txns that are not yet present in the txnCache. LastLogClk indicates the latest txn known to the Replicator from each partition.
	for id, part := range repl.localPartitions {
		part.SendLoggerRequest(LoggerRequest{LogRequestArgs: LogTxnArgs{lastClock: repl.lastLogClk[id], ReplyChan: repl.partsChan}})
	}

	/*prevMaxClk := repl.maxCommonClk
	if prevMaxClk == nil {
		prevMaxClk = clocksi.DummyTs
	}
	repl.maxCommonClk = clocksi.HighestTs*/
	repl.maxCommonTs = math.MaxInt64
	ourReplicaID := shared.SortedReplicaID
	nPartsWithTxn := 0 //TODO: REMOVE, DEBUG
	//Receive replies and cache and also determines the latest common (to all partitions) clk
	for id := uint64(0); id < nGoRoutines; id++ {
		reply := <-repl.partsChan
		intPartID := int(reply.partID)
		if len(reply.upds) > 0 { //This may happen if the partition didn't receive any updates
			partCache := repl.currTxnCache[intPartID]
			nPartsWithTxn++
			if partCache.Len() > 0 { //Put in overflow, we still have leftovers from last time.
				repl.overflowTxnCache[intPartID].Append(reply.upds)
			} else {
				repl.currTxnCache[intPartID] = tools.ToSliceWithHideable(reply.upds)
			}
			//fmt.Printf("[REPL][GetNewTxns()]Received %d txns from part %d with safe clk %s.\n", len(reply.upds), reply.partID, reply.stableClock.ToString())
		} /*else {
			fmt.Printf("[REPL][GetNewTxns()]No txns from part %d. Safe clk: %s.\n", reply.partID, reply.stableClock.ToString())
		}*/
		repl.lastLogClk[intPartID] = reply.stableClock
		/*if reply.stableClock.IsLower(repl.maxCommonClk) {
			repl.maxCommonClk = reply.stableClock
		}*/
		repl.maxCommonTs = min(repl.maxCommonTs, reply.stableClock.GetPos(ourReplicaID))
	}
	if nPartsWithTxn > 0 {
		repl.allPartsDone = false
		//fmt.Printf("[REPL][GetNewTxns()]Received new txns from %d/%d partitions. Max common clk: %s Prev common clk: %s.\n", nPartsWithTxn, nGoRoutines, repl.maxCommonClk.ToString(), prevMaxClk.ToString())
	} /*else {
		fmt.Printf("[REPL][GetNewTxns()]No new txns from any partition.\n")
	}*/
}

// This new version merges multiple txns that were executed sequentially, without the ts of any other replica changing inbetween txns.
// The channel receives the (merged, if multiple txns can be grouped together) txn, and the number of txns that were merged together (1 if it is a single txn).
// At the end, a 0 is returned to signal that we're done processing.
func (repl *Replicator) prepareData(replyChan chan tools.Pair[RemoteTxn, int]) {
	//Helper variables for the cycle
	var currClk, prevClk clocksi.Timestamp
	var partCache tools.SliceWithHideable[PairClockUpdates]
	var firstEntryClk clocksi.Timestamp
	//var clkCompare , maxClkComp clocksi.TsResult
	startTs := time.Now().UnixNano()

	nParts, ourReplicaID := int(nGoRoutines), shared.SortedReplicaID
	partsForThisClk := tools.NewSliceWithCounter[int](nParts)
	done, nPartsDone, nGroups := false, 0, 0
	partsDone := tools.NewBitSet(int(nGoRoutines))
	nIterations := 0 //Every once in a while, we'll check if some partitions are already done or not. This avoids some non-necessary clk comparisons, buffers resets and similar.
	nInGroup := 1    //We count right away the "first" txn.
	maxGroupSize := 0

	partsForGroup := tools.NewBitSet(nParts)
	maxCommonTs := repl.maxCommonTs
	currTs := int64(math.MaxInt64)
	var prevTs int64 //TODO: Remove, only for debugging purposes.
	var compTs int64

	//fmt.Printf("[REPL][PrepareData]Starting to prepare data. Max common clk: %s.\n", repl.maxCommonClk.ToString())
	//Before starting, check if any partition does not have txns to replicate. We'll mark them as done.
	nEmpty, nHigher := 0, 0
	for id, partC := range repl.currTxnCache {
		/*fmt.Printf("[REPL][PrepareData]Checking partition %d, len %d, hidden len %d, isEmpty %v.\n", id, partC.Len(), partC.LenHiddenHead(), partC.IsEmpty())
		if partC.Len() > 0 {
			if partC.LenHiddenHead() > 0 {
				fmt.Printf("[REPL][PrepareData]First entry: clk %v, nUpds %d. Full entries: %v. Visible entries: %v.\n", partC.Head().clk, len(partC.Head().upds), partC.ToFullSlice(), partC.ToSlice())
			}
		}*/
		//if partC.IsEmpty() || partC.Head().clk.IsHigher(repl.maxCommonClk) { //This partition has no txns to replicate, or all txns have a clk higher than the maximum common clk.
		if partC.IsEmpty() || partC.Head().clk.GetPos(ourReplicaID) > maxCommonTs { //This partition has no txns to replicate, or all txns have a clk higher than the maximum common clk.
			partsDone.Set(id)
			nPartsDone++
			if partC.IsEmpty() {
				nEmpty++
			} else { //If we don't enter here due to partC.IsEmpty(), then it means we entered by the condition of > maxCommonTs.
				nHigher++
			}
		}
	}
	if nPartsDone == nParts { //Nothing to replicate: most likely we didn't get any txns.
		if nEmpty == nParts {
			repl.allPartsDone = true
			//fmt.Printf("[REPL][PrepareData]No data to prepare, all partitions have been fully replicated! Max common clk: %s. Returning.\n", repl.maxCommonClk.ToString())
			//fmt.Printf("[REPL][PrepareData]No data to prepare, all partitions are done! Max common clk: %s. Empty partitions: %d. Not empty but higher than maxCommonClk: %d. Returning.\n", repl.maxCommonClk.ToString(), nEmpty, nHigher)
		} else {
			//fmt.Printf("[REPL][PrepareData]No data that can be prepared, but some partitions still have txns to replicate later. Max common clk: %s. Empty partitions: %d. Not empty but higher than maxCommonClk: %d. Returning.\n", repl.maxCommonClk.ToString(), nEmpty, nHigher)
			fmt.Printf("[REPL][PrepareData]No data that can be prepared, but some partitions still have txns to replicate later. Max common ts: %d. Empty partitions: %d. Not empty but higher than maxCommonTs: %d. Returning.\n", repl.maxCommonTs, nEmpty, nHigher)
		}
		replyChan <- tools.Pair[RemoteTxn, int]{Second: 0}
		return
	}
	//fmt.Printf("[REPL][PrepareData]HighestTs: %s. Max common clk: %s.\n", clocksi.HighestTs.ToString(), repl.maxCommonClk.ToString())

	//TODO: Remove this.
	for id := 0; id < nParts; id++ {
		lastLocalTs := int64(0)
		var prevClk clocksi.Timestamp
		cacheSlice := repl.currTxnCache[id].ToSlice()
		for i, entry := range cacheSlice {
			if entry.clk.GetPos(ourReplicaID) < lastLocalTs {
				panic(fmt.Sprintf("[REPL][PrepareData]Error while checking currTxnCache - partition %d has txns with non-monotonic clk values for our replicaID. Entry %d has ts %d, previous entry had ts %d. Current clk, previous clk: %s, %s. Our replicaID: %d.\n",
					id, i, entry.clk.GetPos(ourReplicaID), lastLocalTs, entry.clk.ToString(), prevClk.ToString(), ourReplicaID))
			}
			lastLocalTs = entry.clk.GetPos(ourReplicaID)
			prevClk = entry.clk
		}
		for _, sliceEntry := range repl.overflowTxnCache[id].ToSlice() {
			for j, entry := range sliceEntry {
				if entry.clk.GetPos(ourReplicaID) < lastLocalTs {
					panic(fmt.Sprintf("[REPL][PrepareData]Error while checking overflowTxnCache - partition %d has txns with non-monotonic clk values for our replicaID. Entry %d has ts %d, previous entry had ts %d. Current clk, previous clk: %s, %s. Our replicaID: %d.\n",
						id, j, entry.clk.GetPos(ourReplicaID), lastLocalTs, entry.clk.ToString(), prevClk.ToString(), ourReplicaID))
				}
				lastLocalTs = entry.clk.GetPos(ourReplicaID)
				prevClk = entry.clk
			}
		}
	}

	prevClk = clocksi.HighestTs
	hasPrevClk := false //Bool to ensure that we process correctly the first txn.
	for !done {         //We have guarantee that there's always at least one txn to replicate until we break out of the cycle.
		//if nIterations > 0 && nIterations%500000 == 0 {
		if nIterations > 0 && nIterations&0x7FFFF == 0 { //Aprox every 500k iterations (524288 to be exact), check this condition. Cheaper than doing %.
			fmt.Printf("[REPL]Prepare data is stuck in an infinite loop? Iteration %d. Parts done: %d/%d. Has prev clk? %t. Prev clk: %s. Max common ts: %d (our sorted replicaID: %d).\n", nIterations, nPartsDone, nParts, hasPrevClk, prevClk.ToString(), repl.maxCommonTs, ourReplicaID)
		}
		currClk, currTs = clocksi.HighestTs, math.MaxInt64
		for id := 0; id < nParts; id++ { //1st phase: find the minimum common clock, and collect partitions involved.
			if !partsDone.GetBit(id) { //PartsDone is true when the partition has no txns left to replicate, or all txns have a local ts higher than the maximum common ts.
				partCache = repl.currTxnCache[id]
				firstEntryClk = partCache.Head().clk
				compTs = firstEntryClk.GetPos(ourReplicaID)
				if compTs < currTs {
					partsForThisClk.Clear()
					partsForThisClk.AddToEnd(id)
					currTs, currClk = compTs, firstEntryClk
				} else if compTs == currTs { //Add this partition
					partsForThisClk.AddToEnd(id)
				} //else: ignore, this partition does not belong to this txn.
				/*clkCompare = firstEntryClk.Compare(currClk)
				if clkCompare == clocksi.LowerTs { //Reset
					partsForThisClk.Clear()
					partsForThisClk.AddToEnd(id)
					currClk = firstEntryClk
				} else if clkCompare == clocksi.EqualTs { //Add this partition
					partsForThisClk.AddToEnd(id)
				} //else: ignore, this partition does not belong to this txn*/
			}
		}

		//maxClkComp = currClk.Compare(repl.maxCommonClk)
		//if maxClkComp == clocksi.EqualTs || maxClkComp == clocksi.LowerTs { //This txn can be replicated
		//if currClk.GetPos(ourReplicaID) <= maxCommonTs { //This txn can be replicated.
		if currTs <= maxCommonTs { //This txn can be replicated.
			//Can't merge together. We first send the previous (possibly merged) txn, and then start a new one.
			//hasPrevClk ensures that on the first iteration we don't send an empty group (as at that time, there's not yet a proper prevClk)
			if (!currClk.IsEqualExceptForSelf(prevClk, ourReplicaID) || nInGroup == REPL_MAX_TXN_MERGE) && hasPrevClk {
				repl.prepareMergedTxnHelper(&partsForGroup, prevClk, nInGroup, replyChan)
				maxGroupSize = max(maxGroupSize, nInGroup)
				nInGroup = 0
				nGroups++
			}
			//Merge txn into the current (possibly new) group.
			nInGroup++
			sliceP := partsForThisClk.ToSlice()
			nPartsDoneThisCycle := 0 //TODO: Remove, debug
			for _, partID := range sliceP {
				partsForGroup.Set(partID)
				partCache = repl.currTxnCache[partID]
				//TODO: UNDO, debug
				last := partCache.GetAndHideHead()
				if last.clk.GetPos(ourReplicaID) != currTs {
					panic(fmt.Sprintf("[REPL][PrepareData]Error - adding in the same txn two partitions with different values for the clk of our replicaID. CurrTs: %d. PrevTs: %d. This partition's txn currTs: %d. Current clock: %s. This partition's clk: %s. Our replicaID: %d. PartID: %d. NPartsDoneThisCycle: %d\n",
						currTs, prevTs, last.clk.GetPos(ourReplicaID), currClk.ToString(), last.clk.ToString(), ourReplicaID, partID, nPartsDoneThisCycle))
				}
				if hasPrevClk && last.clk.GetPos(ourReplicaID) < prevClk.GetPos(ourReplicaID) {
					panic(fmt.Sprintf("[REPL][PrepareData]Error - current txn to replicate has a lower entry value for our replicaID than the previous txn!!! CurrTs: %d. PrevTs: %d. Curr txn clk: %s. Previous txn clk: %s. Our replicaID: %d. PartID: %d. NPartsDoneThisCycle: %d\n",
						currTs, prevTs, last.clk.ToString(), prevClk.ToString(), ourReplicaID, partID, nPartsDoneThisCycle))
				}
				repl.partUpdsBuf[partID].AppendAll(last.upds)
				//repl.partUpdsBuf[partID].AppendAll(partCache.GetAndHideHead().upds)
				if partCache.IsEmpty() {
					next := repl.overflowTxnCache[partID].GetAndHideHead()
					if next == nil {
						partsDone.Set(partID)
						nPartsDone++
						repl.overflowTxnCache[partID].DeepClear() //Very fast to execute, as this is a slice of slices (and with few entries)
						if nPartsDone == nParts {
							done = true
						}
						repl.currTxnCache[partID] = tools.SliceWithHideable[PairClockUpdates]{} //Reset to an empty slice, as we'll be returning the original slice to the logger.
					} else {
						repl.currTxnCache[partID] = tools.ToSliceWithHideable(next)
					}
					repl.logBuffersToReturn.Append(tools.Pair[int, tools.SliceWithHideable[PairClockUpdates]]{First: partID, Second: partCache})
				} else {
					repl.currTxnCache[partID] = partCache
				}
				nPartsDoneThisCycle++
			}
			hasPrevClk, prevClk, prevTs = true, currClk, currTs //We always update prevClk, so that it matches the latest txn put in the group
			partsForThisClk.Clear()                             //Clear the partitions for the next iteration
			nIterations++
			if nIterations&0x3FF == 0 { //We check if some more partitions are already done (i.e., their next clk is too high) aproximately every 1024 iterations. Cheaper than doing %.
				for i := 0; i < nParts; i++ {
					//if !partsDone.GetBit(i) && !repl.currTxnCache[i].Get(0).clk.IsLowerOrEqual(repl.maxCommonClk) {
					if !partsDone.GetBit(i) && repl.currTxnCache[i].Get(0).clk.GetPos(ourReplicaID) > maxCommonTs {
						partsDone.Set(i)
						nPartsDone++
					}
				}
			}
		} else {
			currClk = prevClk //Just for printing purposes, so that the clock we print is the last one replicated.
			done = true
		}
	}
	//if hasPrevClk && nInGroup > 0 { //There's at least one txn that is OK to send.
	if hasPrevClk { //There'll always be at least one txn left to send, as when iterating, the currentTxn is always added to the group after the check for prepareMergedTxnHelper.
		repl.prepareMergedTxnHelper(&partsForGroup, prevClk, nInGroup, replyChan)
		maxGroupSize = max(maxGroupSize, nInGroup)
		nGroups++
	}
	//fmt.Printf("[REPL][PrepareData]Done preparing data. Total txns prepared: %d. All partitions finish? %d==%d. Max common clk: %s. Curr clk: %s. Current time: %s\n", nIterations, nPartsDone, nParts, repl.maxCommonClk.ToString(), currClk.ToString(), time.Now().Format("15:04:05.000"))
	end := time.Now()
	fmt.Printf("[REPL][PrepareData]Done preparing data. Total txns prepared: %d. Total groups prepared: %d. Biggest group size: %d. All partitions finish? %d==%d. Max common ts: %d. Our replicaID: %d. Last clock replicated: %s. Current time: %s. Time taken: %dms.\n",
		nIterations, nGroups, maxGroupSize, nPartsDone, nParts, maxCommonTs, repl.replicaID, currClk.ToString(), end.Format("15:04:05.000"), (end.UnixNano()-startTs)/1000000)
	replyChan <- tools.Pair[RemoteTxn, int]{Second: 0} //Indicates that we have sent all txns for this cycle.
}

func (repl *Replicator) prepareMergedTxnHelper(bitset *tools.BitSet, prevClk clocksi.Timestamp, nInGroup int, replyChan chan tools.Pair[RemoteTxn, int]) {
	partsForGroup := *bitset
	txns := make(map[int][]crdt.UpdateObjectParams, partsForGroup.GetNBitsSet(int(nGoRoutines)))
	for i := 0; i < int(nGoRoutines); i++ {
		if partsForGroup.GetBit(i) {
			txns[i] = repl.partUpdsBuf[i].CopyGoSlice()
			repl.partUpdsBuf[i].Clear()
		}
	}
	//fmt.Printf("[REPL][prepareMergedTxnHelper]Prepared merged txn with %d merged txns, involving %d partitions. Clk: %s. Sending to channel.\n", nInGroup, partsForGroup.GetNBitsSet(0), prevClk.ToString())
	replyChan <- tools.Pair[RemoteTxn, int]{First: RemoteTxn{Clk: prevClk, Upds: txns, SenderID: repl.replicaID}, Second: nInGroup}
	partsForGroup.Reset()
	*bitset = partsForGroup
}

func (repl *Replicator) returnBuffersToLog() {
	copySlice := repl.logBuffersToReturn.Copy()
	repl.logBuffersToReturn.Clear()
	for i := 0; i < len(repl.overflowTxnCache); i++ {
		repl.overflowTxnCache[i].ShiftElementsLeft() //This is very cheap (quick), as it is a slice of slices - and the (inside) slices will be very few.
	}
	go repl.returnBuffersToLogHelper(copySlice.ToSlice())
}

func (repl *Replicator) returnBuffersToLogHelper(buffers []tools.Pair[int, tools.SliceWithHideable[PairClockUpdates]]) {
	for _, buffer := range buffers {
		buffer.Second.DeepClear()
		//Sanity check is OK.
		/*fullSlice := buffer.Second.ToFullSlice()
		if len(fullSlice) != cap(fullSlice) {
			panic(fmt.Sprintf("[REPL][returnBuffersToLogHelper]Broken mechanisms of SliceWithHideable: len of full slice and cap don't match. Len/cap: %d, %d.\n", len(fullSlice), cap(fullSlice)))
		}
		for i, entry := range fullSlice {
			if entry.clk != nil || entry.upds != nil {
				panic(fmt.Sprintf("[REPL][returnBuffersToLogHelper]Broken DeepClear() of SliceWithHideable: found an element that isn't nil. Index: %d. Clk: %v. Upds: %v.\n", i, entry.clk, entry.upds))
			}
		}*/
		//fmt.Printf("[REPL][returnBuffersToLogHelper]Sanity check of buffer DeepClear is OK. Returning buf with len, hidden len, cap: %d, %d, %d.\n", buffer.Second.Len(), buffer.Second.LenHiddenHead(), buffer.Second.Cap())
		repl.localPartitions[buffer.First].SendLoggerRequest(LoggerRequest{LogRequestArgs: LogBufferReturnArgs{Buf: buffer.Second.ToFullSlice()}})
	}
}

func (repl *Replicator) receiveRemoteTxns() {
	fmt.Println("[REPL]Ready to receive requests from RabbitMQ.")
	for {
		//fmt.Println("[REPL]Iterating receiveRemoteTxns. Ts:", time.Now().String())
		utilities.FancyInfoPrint(utilities.REPL_PRINT, repl.replicaID, "iterating receiveRemoteTxns")
		remoteReq := repl.remote.GetNextRemoteRequest()
		//fmt.Println("[REPL]Received something from RabbitMQ, handling request.")
		repl.handleRemoteRequest(remoteReq)
		//fmt.Println("[REPL]Finish interating receiveRemoteTxns. Ts:", time.Now().String())
	}
}

// TODO: Maybe one day have a direct communication channel between TM and ReplicatorGroup?
func (repl *Replicator) handleRemoteRequest(remoteReq ReplicatorMsg) {
	switch typedReq := remoteReq.(type) {
	case StableClock:
		//fmt.Printf("[REPL]Forwarding TMRemoteClk to TM. Time: %s. Clk: %d. ReplicaID: %d\n", time.Now().String(), typedReq.Ts, typedReq.SenderID)
		repl.tm.SendRemoteMsg(TMRemoteClk{ReplicaID: typedReq.SenderID, StableTs: typedReq.Ts})
	//fmt.Println("[REPL]Finished sending TMRemoteClk to TM. Ts:", time.Now().String())
	case RemoteTxn:
		//repl.tm.SendRemoteMsg(TMRemoteTxn{ReplicaID: typedReq.SenderID, Clk: typedReq.Clk, Upds: typedReq.Upds})
		//fmt.Printf("[REPL]Forwarding RemoteTxn to TM. Time: %s. NTxns: %d. Clk: %s\n", time.Now().String(), len(typedReq.Upds), typedReq.Clk.ToSortedString())
		repl.tm.SendRemoteMsg(typedReq)
	case RemoteID:
		repl.waitFor--
		fmt.Printf("[REPL]ID received from RabbitMQ: %v, n replicas still left: %d.\n", typedReq, repl.waitFor)
		//repl.receiveHold[typedReq.SenderID] = TMRemoteTxn{}
		repl.tm.SendRemoteMsg(TMReplicaID{ReplicaID: typedReq.SenderID, IP: typedReq.IP, Buckets: typedReq.Buckets})
		if repl.waitFor == 0 {
			repl.allDone = true
			fmt.Println("[REPL]All IDs received, sending signal to TM at", time.Now().Format("15:04:05.000"))
			//go repl.replicateCycle()
			if !repl.replicationStarted.Load() {
				ok := repl.replicationStarted.CompareAndSwap(false, true)
				if ok {
					go repl.doReplication()
				}
			}
			repl.tm.SendRemoteMsg(TMStart{})
		}
	case RemoteTrigger:
		repl.tm.SendRemoteMsg(TMRemoteTrigger{AutoUpdate: typedReq.AutoUpdate, IsGeneric: typedReq.IsGeneric})
	case Join:
		//fmt.Printf("[REPL]HandleJoin: new goroutine")
		go repl.handleJoin(typedReq)
		crdt.NReplicas++
	case RequestBucket:
		//fmt.Printf("[REPL]HandleRequestBucket: new goroutine")
		go repl.handleRequestBucket(typedReq)
	case ReplyJoin:
		fmt.Println("[REPL]Unexpected ReplyJoin", typedReq)
	case ReplyBucket:
		fmt.Println("[REPL]Unexpected ReplyBucket", typedReq)
	case ReplyEmpty:
		fmt.Println("[REPL]Unexpected ReplyEmpty")
	default:
		utilities.FancyErrPrint(utilities.REPL_PRINT, repl.replicaID, "failed to process remoteConnection message - unknown msg type.", fmt.Sprintf("%T", typedReq))
		//utilities.FancyErrPrint(utilities.REPL_PRINT, repl.replicaID, typedReq)
		//fmt.Printf("%T\n", typedReq)
	}
	utilities.FancyInfoPrint(utilities.REPL_PRINT, repl.replicaID, "receiveRemoteTxns finished processing request")
}

/***** JOINING EXISTING SERVERS LOGIC *****/

/***** Existing server logic *****/

func (repl *Replicator) handleJoin(req Join) {
	joinChan := make(chan TimestampPartIdPair, len(repl.localPartitions))
	repl.tm.mat.SendRequestToAllChannels(MaterializerRequest{MatCommitedClkArgs{ReplyChan: joinChan}})
	replyJoin := ReplyJoin{SenderID: repl.replicaID, CommonBkts: req.CommonBkts,
		Clks: make([]clocksi.Timestamp, len(repl.localPartitions)), ReqIP: localRabbitMQIP}

	sendTo := repl.remote.AddReplica(req.ReqIP, repl.buckets, req.SenderID)
	for range repl.localPartitions {
		reply := <-joinChan
		replyJoin.Clks[reply.partID] = reply.Timestamp
	}
	repl.tm.SendRemoteMsg(TMReplicaID{ReplicaID: req.SenderID})
	repl.remote.SendReplyJoin(replyJoin, sendTo)
}

func (repl *Replicator) handleRequestBucket(req RequestBucket) {
	bktMap := make(map[string]struct{}, len(req.Buckets))
	for _, bkt := range req.Buckets {
		bktMap[bkt] = struct{}{}
	}
	tmRequest := TMGetSnapshot{Buckets: bktMap, ReplyChan: make(chan TMGetSnapshotReply)}
	repl.tm.SendRemoteMsg(tmRequest)
	snapshotReply := <-tmRequest.ReplyChan
	replyBucket := ReplyBucket{SenderID: repl.replicaID, PartStates: snapshotReply.PartStates, Clk: snapshotReply.Timestamp}
	repl.remote.SendReplyBucket(replyBucket, req.ReqIP)
}

/***** New server logic *****/

func (repl *Replicator) handleReplyJoin(req *ReplyJoin) {
	fmt.Println("Handling replyJoin")
	repl.holdReplyJoins[repl.nHoldJoins] = req
	repl.waitFor--
	repl.nHoldJoins++
	repl.tm.SendRemoteMsg(TMReplicaID{ReplicaID: req.SenderID})
	if repl.waitFor == 0 {
		//Got all the replies, so we can now ask for the buckets
		repl.askBuckets()
	}
}

func (repl *Replicator) handleReplyBucket(req *ReplyBucket) {
	//Need to count how many were received in order to, at the end, return to the "normal replicator state"
	repl.tm.SendRemoteMsg(TMApplySnapshot{Timestamp: req.Clk, PartStates: req.PartStates})
	repl.waitFor--
	if repl.waitFor == 0 {
		repl.allDone = true
	}
}

func (repl *Replicator) handleReplyEmpty() {
	repl.waitFor--
	if repl.waitFor == 0 && repl.nHoldJoins == 0 {
		//Everyone replied empty. Thus, no need to ask for buckets
		repl.allDone = true
		fmt.Println("ReplyEmpty, setting all done to true")
	} else if repl.waitFor == 0 {
		fmt.Println("Hold joins:", len(repl.holdReplyJoins), repl.holdReplyJoins)
		//At least one of the replies wasn't empty, thus we should ask for buckets
		repl.askBuckets()
	}
	fmt.Println("ReplyEmpty")
}

func (repl *Replicator) createConnAndReplyEmpty(req *Join) {
	sendTo := repl.remote.AddReplica(req.ReqIP, repl.buckets, req.SenderID)
	repl.remote.SendReplyEmpty(sendTo)
}

func (repl *Replicator) joinGroup() {
	fmt.Println("[REPL]joinGroup")
	go repl.queueRemoteRequests()
	repl.waitFor = len(repl.remote.conns)
	crdt.NReplicas = int32(repl.waitFor + 1)
	repl.holdReplyJoins = make([]*ReplyJoin, repl.waitFor)
	fmt.Println("[REPL]Requesting remoteGroup to send join")
	go repl.remote.SendJoin(repl.buckets, repl.replicaID)
}

// While the replica is in joining process, this method is used to queue any non-join related msg to a queue.
// Join msgs are processed differently depending on its type.
func (repl *Replicator) queueRemoteRequests() {
	var msg ReplicatorMsg
	for !repl.allDone {
		msg = repl.remote.GetNextRemoteRequest()
		switch typedMsg := msg.(type) {
		case *ReplyJoin:
			fmt.Println("[REPL]ReplyJoin")
			repl.handleReplyJoin(typedMsg)
		case *ReplyBucket:
			fmt.Println("[REPL]ReplyBucket")
			repl.handleReplyBucket(typedMsg)
		case *Join:
			fmt.Println("[REPL]Join")
			//fmt.Println(typedMsg.ReplyID)
			//repl.remoteConn.SendReplyEmpty(typedMsg.ReplyID)
			repl.createConnAndReplyEmpty(typedMsg)
		case *ReplyEmpty:
			fmt.Println("[REPL]ReplyEmpty")
			//Another replica that is also joining
			repl.handleReplyEmpty()
		default:
			fmt.Println("[REPL]Default")
			repl.holdMsgs = append(repl.holdMsgs, msg)
		}
	}
	fmt.Println("Leaving queueRemoteRequests")
	repl.tm.SendRemoteMsg(TMStart{})
	//Handle messages on hold
	for _, holdReq := range repl.holdMsgs {
		repl.handleRemoteRequest(holdReq)
	}
	repl.JoinInfo = JoinInfo{}
	go repl.receiveRemoteTxns()
	go repl.replicateCycle()
}

func (repl *Replicator) askBuckets() {
	//For each bucket, choose the replica with highest clock.
	//If there's no clock for a bucket, then we can assume that bucket has no content yet anywhere.
	bestBktClks := make(map[string]clocksi.Timestamp)
	askTo := make(map[string]string) //bucket -> ip
	repl.holdReplyJoins = repl.holdReplyJoins[0:repl.nHoldJoins]
	for _, replyJoin := range repl.holdReplyJoins {
		for i, clk := range replyJoin.Clks {
			bkt := replyJoin.CommonBkts[i]
			if clk.IsHigher(bestBktClks[bkt]) {
				bestBktClks[bkt] = clk
				askTo[bkt] = replyJoin.ReqIP
			}
		}
	}
	//"Invert" askTo, i.e., get the mapping of replica -> bkts.
	replicaToBkt := make(map[string][]string)
	for bkt, replica := range askTo {
		replicaToBkt[replica] = append(replicaToBkt[replica], bkt)
	}
	repl.waitFor = len(replicaToBkt)
	ownIP := localRabbitMQIP
	//Finally, send requestBkt msgs
	for askReplicaIP, buckets := range replicaToBkt {
		repl.remote.SendRequestBucket(RequestBucket{Buckets: buckets, SenderID: repl.replicaID, ReqIP: ownIP}, askReplicaIP)
	}
}
