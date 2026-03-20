package components

import (
	fmt "fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"potionDB/potionDB/utilities"
	"potionDB/shared/shared"
	"time"

	"github.com/AndreRijo/go-tools/src/tools"
)

type Replicator struct {
	tm               *TransactionManager //to send request to downstream transactions
	localPartitions  []Logger
	currTxnCache     []tools.SliceWithHideable[PairClockUpdates]   //First index: partitionID. Contains the oldest slice of txns obtained from each log.
	overflowTxnCache []tools.SliceWithHideable[[]PairClockUpdates] //Contains the non-oldest slices of txns obtained from each log.
	lastLogClk       []clocksi.Timestamp                           //The last received stable clk from each partition, in order to what clock to request from each partition.
	maxCommonClk     clocksi.Timestamp                             //The highest clk common to all replicas.
	remote           *RemoteGroup
	started          bool
	replicaID        uint16
	buckets          []string
	JoinInfo
	allReplicaIDs []uint16 //Stores the replicaIDs of all replicas
	partsChan     chan StableClkUpdatesPair

	//New things added
	partUpdsBuf        []tools.SliceWithCounter[crdt.UpdateObjectParams]                                  //Re-usable buffer that holds upds (of a txn/merged txn) per partition.
	logBuffersToReturn tools.SliceWithCounter[tools.Pair[int, tools.SliceWithHideable[PairClockUpdates]]] //Buffers to return to the logger, when it is convenient.
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
	//tsSendDelay time.Duration = 2000 //milliseconds
	tsSendDelay time.Duration = 20000
	//tsSendDelay         time.Duration = 500
	cacheInitialSize           = 100
	toSendInitialSize          = 10
	joinHoldInitialSize        = 100
	DO_JOIN                    = "doJoin"
	MAX_TXN_MERGE              = 1000
	OVERFLOW_INITIAL_SIZE      = 3
	PART_UPDS_BUF_INITIAL_SIZE = 100
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
		fmt.Println("[REPL]Reset complete.")
	}
}

func (repl *Replicator) Initialize(tm *TransactionManager, loggers []Logger, buckets []string, replicaID uint16) {
	if !shared.IsReplDisabled {
		if !repl.started {
			repl.tm = tm
			repl.started = true
			repl.localPartitions = loggers
			bucketsToListen := buckets

			remoteConn := CreateRemoteGroupStruct(bucketsToListen, replicaID)
			repl.remote = remoteConn
			repl.buckets = bucketsToListen
			repl.replicaID = replicaID
			repl.partsChan = make(chan StableClkUpdatesPair, nGoRoutines)
			//TODO: Other initializations?
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
				remoteConn.sendReplicaID(bucketsToListen, localPotionIP)
				go repl.receiveRemoteTxns()
				//go repl.replicateCycle()
				//If there's no other replica, we can start right away
				if len(remoteConn.conns) == 0 {
					fmt.Println("[REPL] PotionDB in single server mode.")
					repl.allDone = true
					go repl.replicateCycle()
					go repl.tm.SendRemoteMsg(TMStart{}) //Different thread to avoid blocking
				}
			}
		}
	} else {
		fmt.Println("[REPL] Warning - replicator is disabled. PotionDB started in single server mode.")
		go tm.SendRemoteMsg(TMStart{})
	}
}

func (repl *Replicator) replicateCycle() {
	replSinceLastGC := false //Unused for now. In the future, if we need to do some cleaning/GC, we can use this to detect when that might be needed.
	count, start, finish, toSleep, taken := time.Duration(0), int64(0), int64(0), time.Duration(0), time.Duration(0)
	var startFull time.Time
	nTxns, prevNTxns, nTotalTxns := 0, 0, 0

	dataChan := make(chan tools.Pair[RemoteTxn, int], 50) //Channel to receive data from prepareData. Size is somewhat arbitrary.

	for {
		startFull = time.Now()
		start = startFull.UnixNano()

		fmt.Printf("[REPL]Requesting new txns...\n")
		repl.getNewTxns()
		fmt.Printf("[REPL]Requesting data preparation...\n")
		go repl.prepareData(dataChan)
		count++
		for pair := <-dataChan; pair.Second != 0; pair = <-dataChan { //Keep receiving until prepareData signals that we're done (i.e., return 0)
			fmt.Printf("[REPL]Got txn/grouped txn from dataChan, with %d merged txns. Total so far: %d. Clock: %s. NParts of txn: %d.\n", pair.Second, nTxns+pair.Second, pair.First.Clk.ToString(), len(pair.First.Upds))
			replSinceLastGC = true
			if pair.Second != 0 {
				repl.remote.SendTxn(pair.First)
				nTxns += pair.Second
			}
		}
		//Send clock to ensure all replicas receive the latest clock.
		//This is also used to know that the last transaction sent has ended.
		fmt.Printf("[REPL]Sending stableClk.\n")
		repl.remote.SendStableClk(repl.maxCommonClk.GetPos(shared.SortedReplicaID))
		finish = time.Now().UnixNano()
		if nTxns > 0 {
			fmt.Printf("[REPL]Requested sending of %d txns, at %s. Took %dms preparing.\n", nTxns, time.Now().Format("15:04:05.000"), (finish-start)/1000000)
			repl.returnBuffersToLog()
		} else if nTxns == 0 && replSinceLastGC { //Take the opportunity to do some GC. We would be sleeping this routine anyway.
			repl.cleanState()
			replSinceLastGC = false
		}
		finish = time.Now().UnixNano()
		taken = time.Duration((finish - start) / 1000000)
		toSleep = tsSendDelay - taken
		if nTxns > 0 && toSleep > 10 && taken*9 < tsSendDelay { //Idea: replication was very fast, so PotionDB is likely under light load. Attempt early replication.
			fmt.Printf("[REPL]Last replication of %d txns was fast (%dms to prepare), will sleep short time.\n", nTxns, taken)
			time.Sleep(tools.Max(tsSendDelay/10, 100) * time.Millisecond)
		} else if toSleep > 10 {
			time.Sleep(toSleep * time.Millisecond)
		}
		if nTxns == 0 && (prevNTxns > 0 || count*tsSendDelay%60000 == 0) {
			fmt.Println("[REPL]No txns to send.")
		}
		prevNTxns = nTxns
		nTxns = 0
	}
	ignore(nTotalTxns)
}

func (repl *Replicator) cleanState() {
	for i := 0; i < len(repl.partUpdsBuf); i++ {
		repl.partUpdsBuf[i].DeepClear()
	}
}

func (repl *Replicator) getNewTxns() {
	//Request for new txns that are not yet present in the txnCache. LastLogClk indicates the latest txn known to the Replicator from each partition.
	for id, part := range repl.localPartitions {
		part.SendLoggerRequest(LoggerRequest{LogRequestArgs: LogTxnArgs{lastClock: repl.lastLogClk[id], ReplyChan: repl.partsChan}})
	}

	repl.maxCommonClk = clocksi.HighestTs
	nPartsWithTxn := 0 //TODO: DEBUG
	//Receive replies and cache and also determines the latest common (to all partitions) clk
	for id := uint64(0); id < nGoRoutines; id++ {
		reply := <-repl.partsChan
		if len(reply.upds) > 0 { //This may happen if the partition didn't receive any updates
			partCache := repl.currTxnCache[int(reply.partID)]
			nPartsWithTxn++
			if partCache.Len() > 0 { //Put in overflow, we still have leftovers from last time.
				repl.overflowTxnCache[int(reply.partID)].Append(reply.upds)
			} else {
				repl.currTxnCache[int(reply.partID)] = tools.ToSliceWithHideable(reply.upds)
			}
			for i, upd := range reply.upds {
				if upd.clk == nil {
					fmt.Printf("[REPL][GetNewTxns()]WARNING - Received a nil clk for part %d, pos %d NUpds: %d!!!\n", reply.partID, i, len(upd.upds))
				}
			}
		}
		repl.lastLogClk[int(reply.partID)] = reply.stableClock
		if reply.stableClock.IsLower(repl.maxCommonClk) {
			repl.maxCommonClk = reply.stableClock
		}
	}
	fmt.Printf("[REPL][GetNewTxns()]Received new txns from %d/%d partitions. Max common clk: %s.\n", nPartsWithTxn, nGoRoutines, repl.maxCommonClk.ToString())
}

// This new version merges multiple txns that were executed sequentially, without the ts of any other replica changing inbetween txns.
// The channel receives the (merged, if multiple txns can be grouped together) txn, and the number of txns that were merged together (1 if it is a single txn).
// At the end, a 0 is returned to signal that we're done processing.
func (repl *Replicator) prepareData(replyChan chan tools.Pair[RemoteTxn, int]) {
	//Helper variables for the cycle
	var currClk, prevClk clocksi.Timestamp
	var partCache tools.SliceWithHideable[PairClockUpdates]
	var firstEntryClk clocksi.Timestamp
	var clkCompare, maxClkComp clocksi.TsResult

	nParts, ourReplicaID := int(nGoRoutines), shared.SortedReplicaID
	partsForThisClk := tools.NewSliceWithCounter[int](nParts)
	done, nPartsDone := false, 0
	partsDone := tools.NewBitSet(int(nGoRoutines))
	nIterations := 0 //Every once in a while, we'll check if some partitions are already done or not. This avoids some non-necessary clk comparisons, buffers resets and similar.
	nInGroup := 1    //We count right away the "first" txn.

	partsForGroup := tools.NewBitSet(nParts)

	//fmt.Printf("[REPL][PrepareData]Starting to prepare data. Max common clk: %s.\n", repl.maxCommonClk.ToString())
	//Before starting, check if any partition does not have txns to replicate. We'll mark them as done.
	for id, partC := range repl.currTxnCache {
		/*fmt.Printf("[REPL][PrepareData]Checking partition %d, len %d, hidden len %d, isEmpty %v.\n", id, partC.Len(), partC.LenHiddenHead(), partC.IsEmpty())
		if partC.Len() > 0 {
			if partC.LenHiddenHead() > 0 {
				fmt.Printf("[REPL][PrepareData]First entry: clk %v, nUpds %d. Full entries: %v. Visible entries: %v.\n", partC.Head().clk, len(partC.Head().upds), partC.ToFullSlice(), partC.ToSlice())
			}
		}*/
		if partC.IsEmpty() || partC.Head().clk.IsHigher(repl.maxCommonClk) { //This partition has no txns to replicate, or all txns have a clk higher than the maximum common clk.
			partsDone.Set(id)
			nPartsDone++
		}
	}
	if nPartsDone == nParts { //Nothing to replicate: most likely we didn't get any txns.
		//fmt.Printf("[REPL][PrepareData]No data to prepare, all partitions are done! Max common clk: %s. Returning.\n", repl.maxCommonClk.ToString())
		replyChan <- tools.Pair[RemoteTxn, int]{Second: 0}
		return
	}
	//fmt.Printf("[REPL][PrepareData]HighestTs: %s. Max common clk: %s.\n", clocksi.HighestTs.ToString(), repl.maxCommonClk.ToString())

	prevClk = clocksi.HighestTs
	hasPrevClk := false //Bool to ensure that we process correctly the first txn.
	for !done {         //We have guarantee that there's always at least one txn to replicate until we break out of the cycle.
		/*if nIterations > 0 && nIterations%1000000 == 0 {
			fmt.Printf("[REPL]Prepare data is stuck in an infinite loop? Iteration %d. Parts done: %d/%d. Has prev clk? %t. Prev clk: %s. Max common clk: %s.\n", nIterations, nPartsDone, nParts, hasPrevClk, prevClk.ToString(), repl.maxCommonClk.ToString())
		}*/
		currClk = clocksi.HighestTs
		for id := 0; id < nParts; id++ { //1st phase: find the minimum common clock, and collect partitions involved.
			if partsDone.GetBit(id) { //This partition has no txns left to replicate, or all txns have a clk higher than the maximum common clk.
				continue
			}
			partCache = repl.currTxnCache[id]
			firstEntryClk = partCache.Head().clk
			clkCompare = firstEntryClk.Compare(currClk)
			if clkCompare == clocksi.LowerTs { //Reset
				partsForThisClk.Clear()
				partsForThisClk.AddToEnd(id)
				currClk = firstEntryClk
			} else if clkCompare == clocksi.EqualTs { //Add this partition
				partsForThisClk.AddToEnd(id)
			} //else: ignore, this partition does not belong to this txn
		}

		maxClkComp = currClk.Compare(repl.maxCommonClk)
		/*if maxClkComp == clocksi.EqualTs || maxClkComp == clocksi.LowerTs { //This txn can be replicated
			if currClk.IsEqualExceptForSelf(prevClk, ourReplicaID) { //Can merge together with current txn
				nInGroup++
				sliceP := partsForThisClk.ToSlice()
				for _, partID := range sliceP {
					partsForGroup.Set(partID)
					partCache = repl.currTxnCache[partID]
					repl.partUpdsBuf[partID].AppendAll(partCache.GetAndHideHead().upds)
					if partCache.IsEmpty() {
						next := repl.overflowTxnCache[partID].GetAndHideHead()
						if next == nil {
							partsDone.Set(partID)
							nPartsDone++
							repl.overflowTxnCache[partID].DeepClear() //Very fast to execute, as this is a slice of slices (and with few entries)
							if nPartsDone == nParts {
								done = true
							}
						} else {
							repl.currTxnCache[partID] = tools.ToSliceWithHideable(next)
						}
						repl.logBuffersToReturn.Append(tools.Pair[int, tools.SliceWithHideable[PairClockUpdates]]{First: partID, Second: partCache})
					} else {
						repl.currTxnCache[partID] = partCache
					}
				}
				if nInGroup == MAX_TXN_MERGE {
					repl.prepareMergedTxnHelper(&partsForGroup, currClk, nInGroup, replyChan)
					nInGroup = 0
				}
			} else if hasPrevClk { //Send previous (possibly merged) txn, start new one.
				repl.prepareMergedTxnHelper(&partsForGroup, prevClk, nInGroup, replyChan)
				nInGroup, prevClk = 1, currClk //Counting with the current txn (currClk)
			} else { //We just started the cycle. Keep iterating and do nothing.
				hasPrevClk, prevClk = true, currClk
			}
			partsForThisClk.Clear() //Clear the partitions for the next iteration
			nIterations++
			prevClk = currClk          //We always update prevClk, so that it matches the latest txn put in the group
			if nIterations%1000 == 0 { //We check if some more partitions are already done (i.e., their next clk is too high).
				for i := 0; i < nParts; i++ {
					if !partsDone.GetBit(i) && !repl.currTxnCache[i].Get(0).clk.Copy().IsLowerOrEqual(repl.maxCommonClk) {
						partsDone.Set(i)
						nPartsDone++
					}
				}
			}
		} else {
			done = true
		}*/
		if maxClkComp == clocksi.EqualTs || maxClkComp == clocksi.LowerTs { //This txn can be replicated
			//Can't merge together. We first send the previous (possibly merged) txn, and then start a new one.
			//hasPrevClk ensures that on the first iteration we don't send an empty group (as at that time, there's not yet a proper prevClk)
			if (!currClk.IsEqualExceptForSelf(prevClk, ourReplicaID) || nInGroup == MAX_TXN_MERGE) && hasPrevClk {
				repl.prepareMergedTxnHelper(&partsForGroup, prevClk, nInGroup, replyChan)
				nInGroup = 0
			}
			//Merge txn into the current (possibly new) group.
			nInGroup++
			sliceP := partsForThisClk.ToSlice()
			for _, partID := range sliceP {
				partsForGroup.Set(partID)
				partCache = repl.currTxnCache[partID]
				repl.partUpdsBuf[partID].AppendAll(partCache.GetAndHideHead().upds)
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
			}
			hasPrevClk, prevClk = true, currClk //We always update prevClk, so that it matches the latest txn put in the group
			partsForThisClk.Clear()             //Clear the partitions for the next iteration
			nIterations++
			if nIterations%1000 == 0 { //We check if some more partitions are already done (i.e., their next clk is too high).
				for i := 0; i < nParts; i++ {
					if !partsDone.GetBit(i) && !repl.currTxnCache[i].Get(0).clk.Copy().IsLowerOrEqual(repl.maxCommonClk) {
						partsDone.Set(i)
						nPartsDone++
					}
				}
			}
		} else {
			done = true
		}
	}
	//if hasPrevClk && nInGroup > 0 { //There's at least one txn that is OK to send.
	if hasPrevClk { //There'll always be at least one txn left to send, as when iterating, the currentTxn is always added to the group after the check for prepareMergedTxnHelper.
		repl.prepareMergedTxnHelper(&partsForGroup, prevClk, nInGroup, replyChan)
	}
	//fmt.Printf("[REPL][PrepareData]Done preparing data. Total txns prepared: %d. All partitions finish? %d==%d. Max common clk: %s. Curr clk: %s.\n", nIterations, nPartsDone, nParts, repl.maxCommonClk.ToString(), currClk.ToString())
	replyChan <- tools.Pair[RemoteTxn, int]{Second: 0} //Indicates that we have sent all txns for this cycle.
}

func (repl *Replicator) prepareMergedTxnHelper(bitset *tools.BitSet, prevClk clocksi.Timestamp, nInGroup int, replyChan chan tools.Pair[RemoteTxn, int]) {
	partsForGroup := *bitset
	txns := make(map[int][]crdt.UpdateObjectParams, partsForGroup.GetNBitsSet(int(nGoRoutines)))
	for i := 0; i < int(nGoRoutines); i++ {
		if partsForGroup.GetBit(i) {
			txns[i] = repl.partUpdsBuf[i].Copy().ToSlice()
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
		repl.localPartitions[buffer.First].SendLoggerRequest(LoggerRequest{LogRequestArgs: LogBufferReturnArgs{Buf: buffer.Second.ToFullSlice()}})
	}
}

//TODO: Receiving.

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
			go repl.replicateCycle()
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
	fmt.Println("joinGroup")
	go repl.queueRemoteRequests()
	repl.waitFor = len(repl.remote.conns)
	crdt.NReplicas = int32(repl.waitFor + 1)
	repl.holdReplyJoins = make([]*ReplyJoin, repl.waitFor)
	fmt.Println("Requesting remoteGroup to send join")
	repl.remote.SendJoin(repl.buckets, repl.replicaID)
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
