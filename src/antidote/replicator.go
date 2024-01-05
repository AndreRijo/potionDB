package antidote

//TODO: As of now, there's no filtering of buckets in the Materializer when replying to a RequestBucket.
//I.e., crdts of all buckets are returned.
//TODO: Think if, for joining replicas, there's any need for asking missing ops.
//Note: If new replicas join while a lot of transactions are happening, the materializer may take wrong decisions.
//This is due to the fact that clockSi, when comparing clocks, assumes both clocks have the same entries.
//Also note that initialDummyTs isn't updated when new replicas are added via Join.

//TODO: In this version of PotionDB, Join no longer needs the list of buckets - it can use the one from RemoteID.
//However, this is a temporary solution for requesting remote objects

//TODO: Replicator's algorithm could be improved.
//Now, we could ask TM for the safe clock, and then ask Log to only send operations until that safe clock.
//This works as every partition will have, at least, until that safe clock.

import (
	fmt "fmt"
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"potionDB/src/shared"
	"potionDB/src/tools"
	"time"
)

type Replicator struct {
	tm              *TransactionManager //to send request to downstream transactions
	localPartitions []Logger
	//remoteReps      []chan ReplicatorRequest
	txnCache map[int][]PairClockUpdates //int: partitionID
	//lastSentClk clocksi.Timestamp
	//receiveReplChan chan ReplicatorRequest
	remoteConn *RemoteGroup
	started    bool
	replicaID  int16
	buckets    []string
	JoinInfo
	allReplicaIDs  []int16           //Stores the replicaIDs of all replicas
	initialDummyTs clocksi.Timestamp //The initial TS used in txnCache to signal that all txns are to be replicated.
	//receiveHold    map[int16]TMRemoteTxn //Holds the current transaction being received for each replicaID.
}

type JoinInfo struct {
	holdMsgs       []ReplicatorMsg
	waitFor        int
	holdReplyJoins []*ReplyJoin
	nHoldJoins     int
	allDone        bool
}

type ReplicatorMsg interface {
	getSenderID() int16
}

/*
type ReplicatorTxn struct {
	Clk      clocksi.Timestamp
	Upds     map[int64][]*UpdateObjectParams
	SenderID int16
	TxnID    int32 //This is filled by remoteConnection.go. When receiving txns, this is used to identify when a different txn is being received.
}

type ReplicatorGroupTxn struct {
	Txns     []RemoteTxnSlice
	SenderID int16
	TxnID    int32 //This is filled by remoteConnection.go. When receiving txns, this is used to identify when a different txn is being received.
}

type RemoteTxnSlice struct {
	Txns []RemoteTxn
	clocksi.Timestamp
}*/

/*
type NewReplicatorRequest struct {
	clocksi.Timestamp
	Upds        []*UpdateObjectParams
	SenderID    int16
	PartitionID int64
	TxnID       int32 //This is filled by remoteConnection.go. When receiving txns, this is used to identify when a different txn is being received.
}*/

type StableClock struct {
	SenderID int16
	Ts       int64
}

type RemoteTxn struct {
	SenderID int16
	Clk      clocksi.Timestamp
	Upds     map[int][]*UpdateObjectParams
	TxnID    int32 //This is filled by remoteConnection.go. Used to identify the transactions
}

type RemoteTxnGroup struct {
	SenderID int16
	Txns     []RemoteTxn
	MinTxnID int32 //This is filled by remoteConnection.go. Used to identify the transactions
	MaxTxnID int32 //This is filled by remoteConnection.go. Used to identify the transactions
}

type RemoteID struct {
	SenderID int16
	Buckets  []string
	IP       string
}

type RemoteTrigger struct {
	AutoUpdate
	IsGeneric bool
}

//All join-related requests include a field to identify in the remoteGroup which connection the reply should be sent to.
type Join struct {
	SenderID   int16
	ReplyID    int16
	CommonBkts []string
	ReqIP      string
}

type ReplyJoin struct {
	SenderID   int16
	ReplyID    int16
	Clks       []clocksi.Timestamp
	CommonBkts []string
	ReqIP      string
}

type RequestBucket struct {
	SenderID int16
	ReplyID  int16
	Buckets  []string
	ReqIP    string
}

type ReplyBucket struct {
	SenderID   int16
	PartStates [][]*proto.ProtoCRDT
	Clk        clocksi.Timestamp
}

type ReplyEmpty struct{}

const (
	tsSendDelay time.Duration = 2000 //milliseconds
	//tsSendDelay         time.Duration = 500
	cacheInitialSize    = 100
	toSendInitialSize   = 10
	joinHoldInitialSize = 100
	DO_JOIN             = "doJoin"
	minTxnsToGroup      = 10   //Only groups txns for sending if it's sending at least 10 txns
	maxTxnsPerGroup     = 1000 //To avoid creating groups too big that take a long time to transfer
	//minTxnsToGroup      = 10  //Only groups txns for sending if it's sending at least 10 txns
	//maxTxnsPerGroup     = 100 //To avoid creating groups too big that take a long time to transfer
)

/*
func (req NewReplicatorRequest) getSenderID() int16 {
	return req.SenderID
}

func (req ReplicatorTxn) getSenderID() int16 {
	return req.SenderID
}

func (req ReplicatorGroupTxn) getSenderID() int16 {
	return req.SenderID
}*/

func (req StableClock) getSenderID() int16 {
	return req.SenderID
}

func (req RemoteTxn) getSenderID() int16 {
	return req.SenderID
}

func (req RemoteTxnGroup) getSenderID() int16 {
	return req.SenderID
}

func (req RemoteID) getSenderID() int16 {
	return req.SenderID
}

//Sender ID is irrelevant. It's just here to implement the ReplicatorMsg interface.
func (req RemoteTrigger) getSenderID() int16 {
	return -1
}

func (req Join) getSenderID() int16 {
	return req.SenderID
}

func (req ReplyJoin) getSenderID() int16 {
	return req.SenderID
}

func (req RequestBucket) getSenderID() int16 {
	return req.SenderID
}

func (req ReplyBucket) getSenderID() int16 {
	return req.SenderID
}

func (req ReplyEmpty) getSenderID() int16 {
	return 0
}

func (repl *Replicator) Reset() {
	if !shared.IsReplDisabled {
		repl.txnCache = make(map[int][]PairClockUpdates)
		dummyTs := clocksi.NewClockSiTimestamp()
		for id := 0; id < int(nGoRoutines); id++ {
			cacheEntry := make([]PairClockUpdates, 1, cacheInitialSize)
			//All txns have a clock higher than this, so the first request is the equivalent of "requesting all commited txns"
			cacheEntry[0] = PairClockUpdates{clk: &dummyTs}
			repl.txnCache[id] = cacheEntry
		}
		fmt.Println("[REPLICATOR]Reset complete.")
	}
}

func (repl *Replicator) Initialize(tm *TransactionManager, loggers []Logger, buckets []string, replicaID int16) {
	if !shared.IsReplDisabled {
		if !repl.started {
			repl.tm = tm
			repl.started = true
			repl.localPartitions = loggers
			//bucketsToListen := make([]string, 1)
			//bucketsToListen[0] = "*"
			bucketsToListen := buckets

			remoteConn, err := CreateRemoteGroupStruct(bucketsToListen, replicaID)
			//TODO: Not ignore err
			ignore(err)
			repl.remoteConn = remoteConn
			repl.txnCache = make(map[int][]PairClockUpdates)
			repl.buckets = bucketsToListen
			repl.initialDummyTs = clocksi.NewClockSiTimestampFromId(replicaID)
			for id := 0; id < int(nGoRoutines); id++ {
				cacheEntry := make([]PairClockUpdates, 1, cacheInitialSize)
				//All txns have a clock higher than this, so the first request is the equivalent of "requesting all commited txns"
				cacheEntry[0] = PairClockUpdates{clk: &repl.initialDummyTs}
				repl.txnCache[id] = cacheEntry
			}
			repl.replicaID = replicaID
			//repl.receiveHold = make(map[int16]TMRemoteTxn)

			//Also skips the joining algorithm if there's no replica to join
			if tools.SharedConfig.GetBoolConfig(DO_JOIN, true) && len(remoteConn.conns) > 0 {
				fmt.Println("[REPLICATOR]Join mode")
				repl.JoinInfo = JoinInfo{holdMsgs: make([]ReplicatorMsg, 0, joinHoldInitialSize), allDone: false, nHoldJoins: 0}
				repl.joinGroup()
			} else {
				//Wait for replicaIDs of existing replicas
				repl.JoinInfo.waitFor = int(remoteConn.nReplicas)
				crdt.NReplicas = int32(remoteConn.nReplicas + 1)
				fmt.Printf("[REPLICATOR]Not doing join, will wait for replicaID of existing replicas. IDs to receive: %d, number of replicas (including self): %d\n",
					repl.JoinInfo.waitFor, crdt.NReplicas)
				remoteConn.sendReplicaID(bucketsToListen, tools.SharedConfig.GetOrDefault("localPotionDBAddress", "localhost:8087"))
				go repl.receiveRemoteTxns()
				go repl.replicateCycle()
				//If there's no other replica, we can start right away
				if len(remoteConn.conns) == 0 {
					fmt.Println("[REPLICATOR] PotionDB in single server mode.")
					repl.initialDummyTs.Update()
					repl.allDone = true
					go repl.tm.SendRemoteMsg(TMStart{}) //Different thread to avoid blocking
				}
			}
		}
	} else {
		fmt.Println("[REPLICATOR] Warning - replicator is disabled. PotionDB started in single server mode.")
		go tm.SendRemoteMsg(TMStart{})
	}
}

func (repl *Replicator) replicateCycle() {
	for {
		if repl.JoinInfo.allDone {
			break
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	count, start, finish, toSleep := time.Duration(0), int64(0), int64(0), time.Duration(0)
	previousLen := 0
	for {
		start = time.Now().UnixNano()
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting replicateCycle")

		repl.getNewTxns()
		count++
		toSend := repl.preparateDataToSend()
		if len(toSend) > 0 {
			fmt.Println("[REPLICATOR]Sending ops.", "Number of remote txns:", len(toSend))
			repl.sendTxns(toSend)
		}
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finishing replicateCycle")

		finish = time.Now().UnixNano()
		toSleep = tsSendDelay - time.Duration((finish-start)/1000000)
		if toSleep > 10 {
			time.Sleep(toSleep * time.Millisecond)
		}
		//time.Sleep(tsSendDelay * time.Millisecond)
		if len(toSend) == 0 && (previousLen > 0 || count*tsSendDelay%60000 == 0) {
			fmt.Println("[REPLICATOR]No ops to send.")
		}
		previousLen = len(toSend)
		//ignore(toSend)
	}
}

func (repl *Replicator) getNewTxns() {
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting getNewTxns()")
	replyChans := make([]chan StableClkUpdatesPair, len(repl.localPartitions))

	//Send request for txns
	for id, part := range repl.localPartitions {
		partEntry := repl.txnCache[id]
		replyChans[id] = make(chan StableClkUpdatesPair)
		lastClk := *partEntry[len(partEntry)-1].clk
		part.SendLoggerRequest(LoggerRequest{
			LogRequestArgs: LogTxnArgs{
				lastClock: lastClk,
				ReplyChan: replyChans[id],
			},
		})
	}
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "getNewTxns() second part")

	//Receive replies and cache them
	for id, replyChan := range replyChans {
		reply := <-replyChan
		cacheEntry := repl.txnCache[id]
		//Remove last entry which is the previous, old, stable clock
		if len(cacheEntry) > 0 {
			cacheEntry = cacheEntry[:len(cacheEntry)-1]
		}
		cacheEntry = append(cacheEntry, reply.upds...)
		repl.txnCache[id] = append(cacheEntry, PairClockUpdates{clk: &reply.stableClock, upds: []*UpdateObjectParams{}})
	}
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "getNewTxns() finish")
}

func (repl *Replicator) preparateDataToSend() (toSend []RemoteTxn) {
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting prepareDataToSend")
	toSend = make([]RemoteTxn, 0, toSendInitialSize)
	foundStableClk := false
	for !foundStableClk {
		minClk := *repl.txnCache[0][0].clk //Using first entry of first partition as initial value
		var txnUpdates map[int][]*UpdateObjectParams = make(map[int][]*UpdateObjectParams)
		//tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "nPartitions:", len(repl.localPartitions))
		for id := 0; id < len(repl.localPartitions); id++ {
			partCache := repl.txnCache[id]

			//tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "non empty cache entry:", *partCache[0].clk, *partCache[0].upds)
			firstEntry := partCache[0]
			clkCompare := (*firstEntry.clk).Compare(minClk)
			if clkCompare == clocksi.LowerTs {
				//Clock update, this partition has no further transactions. If no smaller clk is found, then this must be the last iteration
				if len(firstEntry.upds) == 0 {
					foundStableClk = true
					//Cleaning buffer
					partCache = make([]PairClockUpdates, 1, cacheInitialSize)
					partCache[0] = firstEntry
					repl.txnCache[id] = partCache
				} else {
					foundStableClk = false
				}
				minClk = *firstEntry.clk
				txnUpdates = nil
				txnUpdates = make(map[int][]*UpdateObjectParams)
				txnUpdates[id] = firstEntry.upds
			} else if clkCompare == clocksi.EqualTs {
				txnUpdates[id] = firstEntry.upds
				if len(firstEntry.upds) == 0 {
					foundStableClk = true
					//Cleaning buffer
					partCache = make([]PairClockUpdates, 1, cacheInitialSize)
					partCache[0] = firstEntry
					repl.txnCache[id] = partCache
				}
			}
		}
		if !foundStableClk {
			//Safe to include the actual txn's clock in the list to send. We can remove entry from cache
			tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "appending upds. Upds:", txnUpdates)
			toSend = append(toSend, RemoteTxn{Clk: minClk, Upds: txnUpdates, SenderID: repl.replicaID})
			tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "upds to send:", txnUpdates)
			for id := range txnUpdates {
				repl.txnCache[id] = repl.txnCache[id][1:]
			}
		}
	}
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finished prepareDataToSend")
	return
}

func (repl *Replicator) sendTxns(toSend []RemoteTxn) {

	if len(toSend) >= minTxnsToGroup {
		repl.sendGroupTxns(toSend)
	} else {
		repl.sendIndividualTxns(toSend)
	}

	//repl.sendIndividualTxns(toSend)
	//Send clock to ensure all replicas receive the latest clock.
	//This is also used to know that the last transaction sent has ended.
	repl.remoteConn.SendStableClk(toSend[len(toSend)-1].Clk.GetPos(repl.replicaID))
}

func (repl *Replicator) sendIndividualTxns(toSend []RemoteTxn) {
	fmt.Printf("[REPLICATOR]Sending txns individually (size: %d)\n", len(toSend))
	for _, txn := range toSend {
		repl.remoteConn.SendTxn(txn)
	}
}

func (repl *Replicator) sendGroupTxns(toSend []RemoteTxn) {
	fmt.Printf("[REPLICATOR]Sending txns in group (size: %d). Min: %d, max: %d\n", len(toSend), minTxnsToGroup, maxTxnsPerGroup)
	end := 0
	for len(toSend) > 0 {
		end = tools.MinInt(len(toSend), maxTxnsPerGroup)
		//start, end = currOffset, tools.MinInt(len(toSend)-currOffset, maxTxnsPerGroup)+currOffset //for txns variable
		currTxns := toSend[:end]
		firstClk := currTxns[0].Clk
		for i, txn := range currTxns {
			if !txn.Clk.IsEqualExceptForSelf(firstClk, repl.replicaID) {
				fmt.Printf("[REPLICATOR]Clk of other replica changed. This happened at index %d. Clks. %s, %s\n", i, firstClk.ToSortedString(), txn.Clk.ToSortedString())
				end = i
				currTxns = currTxns[:i]
				//This txn depends on some txn of another replica, thus better not put in this group
				break
			}
		}
		if end < minTxnsToGroup/2 {
			//Send individually, too few transactions.
			fmt.Printf("[REPLICATOR]Sending individual in groupTxns. End: %d, min/2: %d, nTxns: %d\n", end, minTxnsToGroup, len(currTxns))
			for _, txn := range currTxns {
				repl.remoteConn.SendTxn(txn)
			}
		} else {
			//Send the slice
			fmt.Printf("[REPLICATOR]Sending group in groupTxns. Size: %d\n", len(currTxns))
			repl.remoteConn.SendGroupTxn(currTxns)
		}
		toSend = toSend[end:] //Hide positions that were already sent
	}
}

/*
func (repl *Replicator) sendTxns(toSend []RemoteTxn) {
	//Separate each txn into partitions
	//TODO: Batch of transactions for the same partition? Not simple due to clock updates.
	//startTime := time.Now().UnixNano()
	//TODO: It seems like in some situations multiple stable clocks are sent in a row. I should look into this.
	count := 0
	for _, txn := range toSend {
		for partId, upds := range txn.Upds {
			count++
			repl.remoteConn.SendPartTxn(&NewReplicatorRequest{PartitionID: int64(partId), SenderID: repl.replicaID, Timestamp: txn.Timestamp, Upds: upds})
		}
	}
	//Send clock to ensure all replicas receive the latest clock
	repl.remoteConn.SendStableClk(toSend[len(toSend)-1].Timestamp.GetPos(repl.replicaID))
	count++
	//endTime := time.Now().UnixNano()
	//fmt.Printf("[REPLICATOR]Took %d ms to send txns to rabbitMQ. Total msgs sent: %d.\n", (endTime-startTime)/1000000, count)
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finished sendTxns")
}
*/

/*
func (repl *Replicator) groupTxns(txns []RemoteTxn) {
	if len(txns) < minTxnsToGroup {
		return
	}
	start, end := 0, tools.MinInt(len(txns), maxTxnsPerGroup) //for txns variable
	currTxns := txns[start:end]
	currOffset := 0
	firstClk := currTxns[0].Timestamp
	for i, txn := range currTxns[1:] {
		if !txn.IsEqualExceptForSelf(firstClk, repl.replicaID) {
			//This txn depends on some txn of another replica, thus better not put in this group
			currOffset = i
			break
		}
	}
	if currOffset < minTxnsToGroup/2 {
		//Send individually
	} else {
		//Make a group
		txnGroup := RemoteTxnGroup{txns: currTxns}
	}
}
*/

func (repl *Replicator) receiveRemoteTxns() {
	fmt.Println("[Replicator]Ready to receive requests from RabbitMQ.")
	for {
		//fmt.Println("[REPL]Iterating receiveRemoteTxns. Ts:", time.Now().String())
		tools.FancyInfoPrint(tools.REPL_PRINT, repl.replicaID, "iterating receiveRemoteTxns")
		remoteReq := repl.remoteConn.GetNextRemoteRequest()
		//fmt.Println("[REPL]Received something from RabbitMQ, handling request.")
		repl.handleRemoteRequest(remoteReq)
		//fmt.Println("[REPL]Finish interating receiveRemoteTxns. Ts:", time.Now().String())
	}
}

//TODO: Maybe one day have a direct communication channel between TM and ReplicatorGroup?
func (repl *Replicator) handleRemoteRequest(remoteReq ReplicatorMsg) {
	switch typedReq := remoteReq.(type) {
	case *StableClock:
		/*hold := repl.receiveHold[typedReq.SenderID]
		if hold.Clk != nil {
			fmt.Println("[REPL]Sending TMRemoteTxn to TM due to clock. Ts:", time.Now().String())
			repl.tm.SendRemoteMsg(hold)
			fmt.Println("[REPL]Finished sending TMRemoteTxn to TM due to clock. Ts:", time.Now().String())
			repl.receiveHold[typedReq.SenderID] = TMRemoteTxn{}
		}*/
		//fmt.Println("[REPL]Sending TMRemoteClk to TM. Ts:", time.Now().String())
		repl.tm.SendRemoteMsg(TMRemoteClk{ReplicaID: typedReq.SenderID, StableTs: typedReq.Ts})
		//fmt.Println("[REPL]Finished sending TMRemoteClk to TM. Ts:", time.Now().String())
	case *RemoteTxn:
		//repl.tm.SendRemoteMsg(TMRemoteTxn{ReplicaID: typedReq.SenderID, Clk: typedReq.Clk, Upds: typedReq.Upds})
		repl.tm.SendRemoteMsg(*typedReq)
	case *RemoteTxnGroup:
		repl.tm.SendRemoteMsg(*typedReq)
	/*	case *NewReplicatorRequest:
		hold := repl.receiveHold[typedReq.SenderID]
		if typedReq.Timestamp.IsEqual(hold.Clk) {
			hold.Upds[typedReq.PartitionID] = typedReq.Upds
		} else {
			//New transaction, previous transaction is complete.
			//If the previous txn wasn't given yet to the TM, do so now.
			if hold.Clk != nil {
				fmt.Println("[REPL]Sending TMRemoteTxn to TM. Ts:", time.Now().String())
				repl.tm.SendRemoteMsg(hold)
				fmt.Println("[REPL]Finished sending TMRemoteTxn to TM. Ts:", time.Now().String())
			}
			newHold := TMRemoteTxn{
				ReplicaID: typedReq.SenderID,
				Clk:       typedReq.Timestamp,
				Upds:      make([][]*UpdateObjectParams, len(repl.localPartitions)),
			}
			newHold.Upds[typedReq.PartitionID] = typedReq.Upds
			repl.receiveHold[typedReq.SenderID] = newHold
		}
	*/
	case *RemoteID:
		repl.waitFor--
		fmt.Println("ID received from RabbitMQ: ", typedReq)
		//repl.receiveHold[typedReq.SenderID] = TMRemoteTxn{}
		repl.tm.SendRemoteMsg(TMReplicaID{ReplicaID: typedReq.SenderID, IP: typedReq.IP, Buckets: typedReq.Buckets})
		if repl.waitFor == 0 {
			repl.initialDummyTs.Update()
			repl.allDone = true
			fmt.Println("[RC]All IDs received, sending signal to TM.")
			repl.tm.SendRemoteMsg(TMStart{})
		}
	case *RemoteTrigger:
		repl.tm.SendRemoteMsg(TMRemoteTrigger{AutoUpdate: typedReq.AutoUpdate, IsGeneric: typedReq.IsGeneric})
	case *Join:
		go repl.handleJoin(typedReq)
		crdt.NReplicas++
	case *RequestBucket:
		go repl.handleRequestBucket(typedReq)
	case *ReplyJoin:
		fmt.Println("Unexpected ReplyJoin", *typedReq)
	case *ReplyBucket:
		fmt.Println("Unexpected ReplyBucket", *typedReq)
	case *ReplyEmpty:
		fmt.Println("Unexpected ReplyEmpty")
	default:
		tools.FancyErrPrint(tools.REPL_PRINT, repl.replicaID, "failed to process remoteConnection message - unknown msg type.")
		tools.FancyErrPrint(tools.REPL_PRINT, repl.replicaID, typedReq)
		fmt.Printf("%T\n", typedReq)
	}

	//time.Sleep(5000 * time.Millisecond)
	tools.FancyInfoPrint(tools.REPL_PRINT, repl.replicaID, "receiveRemoteTxns finished processing request")
}

/***** JOINING EXISTING SERVERS LOGIC *****/

/***** Existing server logic *****/

func (repl *Replicator) handleJoin(req *Join) {
	joinChan := make(chan TimestampPartIdPair, len(repl.localPartitions))
	repl.tm.mat.SendRequestToAllChannels(MaterializerRequest{MatCommitedClkArgs{ReplyChan: joinChan}})
	replyJoin := ReplyJoin{SenderID: repl.replicaID, CommonBkts: req.CommonBkts,
		Clks: make([]clocksi.Timestamp, len(repl.localPartitions)), ReqIP: tools.SharedConfig.GetConfig("localRabbitMQAddress")}

	sendTo := repl.remoteConn.AddReplica(req.ReqIP, repl.buckets, req.SenderID)
	for range repl.localPartitions {
		reply := <-joinChan
		replyJoin.Clks[reply.partID] = reply.Timestamp
	}
	repl.tm.SendRemoteMsg(TMReplicaID{ReplicaID: req.SenderID})
	repl.remoteConn.SendReplyJoin(replyJoin, sendTo)
}

func (repl *Replicator) handleRequestBucket(req *RequestBucket) {
	bktMap := make(map[string]struct{})
	for _, bkt := range req.Buckets {
		bktMap[bkt] = struct{}{}
	}
	tmRequest := TMGetSnapshot{Buckets: bktMap, ReplyChan: make(chan TMGetSnapshotReply)}
	repl.tm.SendRemoteMsg(tmRequest)
	snapshotReply := <-tmRequest.ReplyChan
	replyBucket := ReplyBucket{SenderID: repl.replicaID, PartStates: snapshotReply.PartStates, Clk: snapshotReply.Timestamp}
	repl.remoteConn.SendReplyBucket(replyBucket, req.ReqIP)
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
	sendTo := repl.remoteConn.AddReplica(req.ReqIP, repl.buckets, req.SenderID)
	repl.remoteConn.SendReplyEmpty(sendTo)
}

func (repl *Replicator) joinGroup() {
	fmt.Println("joinGroup")
	go repl.queueRemoteRequests()
	repl.waitFor = len(repl.remoteConn.conns)
	crdt.NReplicas = int32(repl.waitFor + 1)
	repl.holdReplyJoins = make([]*ReplyJoin, repl.waitFor)
	fmt.Println("Requesting remoteGroup to send join")
	repl.remoteConn.SendJoin(repl.buckets, repl.replicaID)
}

//While the replica is in joining process, this method is used to queue any non-join related msg to a queue.
//Join msgs are processed differently depending on its type.
func (repl *Replicator) queueRemoteRequests() {
	var msg ReplicatorMsg
	for !repl.allDone {
		msg = repl.remoteConn.GetNextRemoteRequest()
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
	ownIP := tools.SharedConfig.GetConfig("localRabbitMQAddress")
	//Finally, send requestBkt msgs
	for askReplicaIP, buckets := range replicaToBkt {
		repl.remoteConn.SendRequestBucket(RequestBucket{Buckets: buckets, SenderID: repl.replicaID, ReqIP: ownIP}, askReplicaIP)
	}
}
