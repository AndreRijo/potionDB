package antidote

//TODO: As of now, there's no filtering of buckets in the Materializer when replying to a RequestBucket.
//I.e., crdts of all buckets are returned.
//TODO: Think if, for joining replicas, there's any need for asking missing ops.

import (
	"clocksi"
	fmt "fmt"
	"proto"
	"shared"
	"strings"
	"time"
	"tools"
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

type NewReplicatorRequest struct {
	clocksi.Timestamp
	Upds        []*UpdateObjectParams
	SenderID    int16
	PartitionID int64
}

type StableClock struct {
	SenderID int16
	Ts       int64
}

type RemoteTxn struct {
	clocksi.Timestamp
	Upds map[int][]*UpdateObjectParams
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
)

func (req NewReplicatorRequest) getSenderID() int16 {
	return req.SenderID
}

func (req StableClock) getSenderID() int16 {
	return req.SenderID
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

func (repl *Replicator) Initialize(tm *TransactionManager, loggers []Logger, replicaID int16) {
	if !shared.IsReplDisabled {
		if !repl.started {
			repl.tm = tm
			repl.started = true
			repl.localPartitions = loggers
			//bucketsToListen := make([]string, 1)
			//bucketsToListen[0] = "*"
			var bucketsToListen []string
			stringBuckets, has := tools.SharedConfig.GetAndHasConfig("buckets")
			if !has {
				bucketsToListen = []string{"*"}
			} else {
				bucketsToListen = strings.Split(stringBuckets, " ")
			}
			remoteConn, err := CreateRemoteGroupStruct(bucketsToListen, replicaID)
			//TODO: Not ignore err
			ignore(err)
			repl.remoteConn = remoteConn
			repl.txnCache = make(map[int][]PairClockUpdates)
			repl.buckets = bucketsToListen
			dummyTs := clocksi.NewClockSiTimestamp(replicaID)
			for id := 0; id < int(nGoRoutines); id++ {
				cacheEntry := make([]PairClockUpdates, 1, cacheInitialSize)
				//All txns have a clock higher than this, so the first request is the equivalent of "requesting all commited txns"
				cacheEntry[0] = PairClockUpdates{clk: &dummyTs}
				repl.txnCache[id] = cacheEntry
			}
			repl.replicaID = replicaID

			//Also skips the joining algorithm if there's no replica to join
			if tools.SharedConfig.GetBoolConfig(DO_JOIN, true) && len(remoteConn.conns) > 0 {
				repl.JoinInfo = JoinInfo{holdMsgs: make([]ReplicatorMsg, 0, joinHoldInitialSize), allDone: false, nHoldJoins: 0}
				repl.joinGroup()
			} else {
				go repl.receiveRemoteTxns()
				go repl.replicateCycle()
			}
		}
	}
}

func (repl *Replicator) replicateCycle() {
	//count := 0
	for {
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting replicateCycle")
		repl.getNewTxns()
		//count++
		toSend := repl.preparateDataToSend()
		if len(toSend) > 0 {
			fmt.Println("[REPLICATOR]Still sending ops.", "Number of remote txns:", len(toSend))
		}
		repl.sendTxns(toSend)
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finishing replicateCycle")
		time.Sleep(tsSendDelay * time.Millisecond)
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
		/*
			var lastClk clocksi.Timestamp
			if len(partEntry) > 0 {
				lastClk = *partEntry[len(partEntry)-1].clk
			} else {
				lastClk = repl.lastSentClk
			}
		*/
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
			toSend = append(toSend, RemoteTxn{Timestamp: minClk, Upds: txnUpdates})
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
	/*
		fmt.Print(tools.REPL_PRINT+string(repl.replicaID)+"]"+tools.DEBUG, "starting sendTxns:[")
		for _, txn := range toSend {
			fmt.Print("{", txn.Timestamp, "|", *txn.Upds, "},")
		}
		fmt.Println("]")
	*/
	/*
		for _, remoteChan := range repl.remoteReps {
			remoteChan <- ReplicatorRequest{SenderID: repl.replicaID, Txns: toSend, StableTs: stableTs}
		}
	*/
	//Separate each txn into partitions
	//TODO: Batch of transactions for the same partition? Not simple due to clock updates.
	//startTime := time.Now().UnixNano()
	count := 0
	for _, txn := range toSend {
		for partId, upds := range txn.Upds {
			count++
			repl.remoteConn.SendPartTxn(&NewReplicatorRequest{PartitionID: int64(partId), SenderID: repl.replicaID, Timestamp: txn.Timestamp, Upds: upds})
		}
		count++
		//TODO: This might not even be necessary to be sent - probably it is enough to send this when there's no upds to send.
		repl.remoteConn.SendStableClk(txn.Timestamp.GetPos(repl.replicaID))
		//fmt.Println("Sending txns")
	}
	//endTime := time.Now().UnixNano()
	//fmt.Printf("[REPLICATOR]Took %d ms to send txns to rabbitMQ. Total msgs sent: %d.\n", (endTime-startTime)/1000000, count)
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finished sendTxns")
}

func (repl *Replicator) receiveRemoteTxns() {
	for {
		tools.FancyInfoPrint(tools.REPL_PRINT, repl.replicaID, "iterating receiveRemoteTxns")
		remoteReq := repl.remoteConn.GetNextRemoteRequest()
		repl.handleRemoteRequest(remoteReq)
	}
}

func (repl *Replicator) handleRemoteRequest(remoteReq ReplicatorMsg) {
	switch typedReq := remoteReq.(type) {
	case *StableClock:
		repl.tm.SendRemoteMsg(TMRemoteClk{
			ReplicaID: typedReq.SenderID,
			StableTs:  typedReq.Ts,
		})
	case *NewReplicatorRequest:
		repl.tm.SendRemoteMsg(TMRemotePartTxn{
			ReplicaID:   typedReq.SenderID,
			PartitionID: typedReq.PartitionID,
			Upds:        typedReq.Upds,
			Timestamp:   typedReq.Timestamp,
		})
	case *Join:
		go repl.handleJoin(typedReq)
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

//TODO: Empty case/first replica to replicate a set of buckets
func (repl *Replicator) handleReplyJoin(req *ReplyJoin) {
	fmt.Println("Handling replyJoin")
	repl.holdReplyJoins[repl.nHoldJoins] = req
	repl.waitFor--
	repl.nHoldJoins++
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
