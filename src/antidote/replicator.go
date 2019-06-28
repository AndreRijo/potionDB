package antidote

import (
	"clocksi"
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
	replicaID  int64
}

type ReplicatorMsg interface {
	getSenderID() int64
}

type NewReplicatorRequest struct {
	clocksi.Timestamp
	Upds        []UpdateObjectParams
	SenderID    int64
	PartitionID int64
}

type StableClock struct {
	SenderID int64
	Ts       int64
}

type RemoteTxn struct {
	clocksi.Timestamp
	Upds *map[int][]UpdateObjectParams
}

const (
	tsSendDelay       time.Duration = 2000 //milliseconds
	cacheInitialSize                = 100
	toSendInitialSize               = 10
)

func (req NewReplicatorRequest) getSenderID() int64 {
	return req.SenderID
}

func (req StableClock) getSenderID() int64 {
	return req.SenderID
}

func (repl *Replicator) Initialize(tm *TransactionManager, loggers []Logger, replicaID int64) {
	if !repl.started {
		repl.tm = tm
		repl.started = true
		//nGoRoutines: number of partitions (defined in Materializer)
		repl.localPartitions = loggers
		//TODO: Actually choose which buckets are to be listen to
		//remoteConn, err := CreateRemoteConnStruct(partitionIDs, replicaID)
		bucketsToListen := make([]string, 1)
		bucketsToListen[0] = "*"
		//remoteConn, err := CreateRemoteGroupStruct(myDefaultIp, defaultIpList, bucketsToListen, replicaID)
		remoteConn, err := CreateRemoteGroupStruct(bucketsToListen, replicaID)
		//TODO: Not ignore err
		ignore(err)
		repl.remoteConn = remoteConn
		repl.txnCache = make(map[int][]PairClockUpdates)
		dummyTs := clocksi.NewClockSiTimestamp(replicaID)
		for id := 0; id < int(nGoRoutines); id++ {
			cacheEntry := make([]PairClockUpdates, 1, cacheInitialSize)
			//All txns have a clock higher than this, so the first request is the equivalent of "requesting all commited txns"
			cacheEntry[0] = PairClockUpdates{clk: &dummyTs}
			repl.txnCache[id] = cacheEntry
			//repl.txnCache[id] = make([]PairClockUpdates, 0, cacheInitialSize)
		}
		//repl.lastSentClk = clocksi.NewClockSiTimestamp(replicaID)
		//repl.receiveReplChan = make(chan ReplicatorRequest)
		repl.replicaID = replicaID
		go repl.receiveRemoteTxns()
		go repl.replicateCycle()
	}
}

func (repl *Replicator) replicateCycle() {
	for {
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting replicateCycle")
		repl.getNewTxns()
		toSend := repl.preparateDataToSend()
		repl.sendTxns(toSend)
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finishing replicateCycle")
		time.Sleep(tsSendDelay * time.Millisecond)
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
		repl.txnCache[id] = append(cacheEntry, PairClockUpdates{clk: &reply.stableClock, upds: &[]UpdateObjectParams{}})
	}
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "getNewTxns() finish")
}

func (repl *Replicator) preparateDataToSend() (toSend []RemoteTxn) {
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting prepareDataToSend")
	toSend = make([]RemoteTxn, 0, toSendInitialSize)
	foundStableClk := false
	for !foundStableClk {
		minClk := *repl.txnCache[0][0].clk //Using first entry of first partition as initial value
		var txnUpdates map[int][]UpdateObjectParams = make(map[int][]UpdateObjectParams)
		//tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "nPartitions:", len(repl.localPartitions))
		for id := 0; id < len(repl.localPartitions); id++ {
			partCache := repl.txnCache[id]

			//tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "non empty cache entry:", *partCache[0].clk, *partCache[0].upds)
			firstEntry := partCache[0]
			clkCompare := (*firstEntry.clk).Compare(minClk)
			if clkCompare == clocksi.LowerTs {
				//Clock update, this partition has no further transactions. If no smaller clk is found, then this must be the last iteration
				if len(*firstEntry.upds) == 0 {
					foundStableClk = true
				} else {
					foundStableClk = false
				}
				minClk = *firstEntry.clk
				txnUpdates = nil
				txnUpdates = make(map[int][]UpdateObjectParams)
				txnUpdates[id] = *firstEntry.upds
			} else if clkCompare == clocksi.EqualTs {
				txnUpdates[id] = *firstEntry.upds
				if len(*firstEntry.upds) == 0 {
					foundStableClk = true
				}
			}
		}
		if !foundStableClk {
			//Safe to include the actual txn's clock in the list to send. We can remove entry from cache
			tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "appending upds. Upds:", txnUpdates)
			toSend = append(toSend, RemoteTxn{Timestamp: minClk, Upds: &txnUpdates})
			tools.FancyWarnPrint(tools.REPL_PRINT, repl.replicaID, "upds to send:", txnUpdates)
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
	for _, txn := range toSend {
		for partId, upds := range *txn.Upds {
			repl.remoteConn.SendPartTxn(&NewReplicatorRequest{PartitionID: int64(partId), SenderID: repl.replicaID, Timestamp: txn.Timestamp, Upds: upds})
		}
		//TODO: This might not even be necessary to be sent - probably it is enough to send this when there's no upds to send.
		repl.remoteConn.SendStableClk(txn.Timestamp.GetPos(repl.replicaID))
	}
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finished sendTxns")
}

func (repl *Replicator) receiveRemoteTxns() {
	for {
		tools.FancyInfoPrint(tools.REPL_PRINT, repl.replicaID, "iterating receiveRemoteTxns")
		//remoteReq := <-repl.receiveReplChan
		remoteReq := repl.remoteConn.GetNextRemoteRequest()
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
		default:
			tools.FancyErrPrint(tools.REPL_PRINT, repl.replicaID, "failed to process remoteConnection message - unknown msg type.")
		}

		tools.FancyInfoPrint(tools.REPL_PRINT, repl.replicaID, "receiveRemoteTxns finished processing request")
	}
}

/*
package antidote

import (
	"clocksi"
	"time"
	"tools"
)

type Replicator struct {
	tm              *TransactionManager //to send request to downstream transactions
	localPartitions []Logger
	//remoteReps      []chan ReplicatorRequest
	txnCache    map[int][]PairClockUpdates //int: partitionID
	lastSentClk clocksi.Timestamp
	//receiveReplChan chan ReplicatorRequest
	remoteConn *RemoteConn
	started    bool
	replicaID  int64
}

type NewReplicatorRequest struct {
	SenderID    int64
	Txns        []NewRemoteTxns
	PartitionID int64
	StableTs    int64
}

type NewRemoteTxns struct {
	clocksi.Timestamp
	Upds []*UpdateObjectParams
}

type ReplicatorRequest struct {
	SenderID int64
	Txns     []RemoteTxns
	StableTs int64
}

type RemoteTxns struct {
	clocksi.Timestamp
	Upds *map[int][]UpdateObjectParams
}

const (
	tsSendDelay       time.Duration = 2000 //milliseconds
	cacheInitialSize                = 100
	toSendInitialSize               = 10
)

func (repl *Replicator) Initialize(tm *TransactionManager, loggers []Logger, replicaID int64) {
	if !repl.started {
		repl.tm = tm
		repl.started = true
		//nGoRoutines: number of partitions (defined in Materializer)
		repl.localPartitions = loggers
		//TODO: Some way to know how many and which remoteReps there is
		//repl.remoteReps = make([]chan ReplicatorRequest, 0)
		remoteConn, err := CreateRemoteConnStruct(replicaID)
		//TODO: Not ignore err
		ignore(err)
		repl.remoteConn = remoteConn
		repl.txnCache = make(map[int][]PairClockUpdates)
		for id := 0; id < int(nGoRoutines); id++ {
			repl.txnCache[id] = make([]PairClockUpdates, 0, cacheInitialSize)
		}
		repl.lastSentClk = clocksi.NewClockSiTimestamp(replicaID)
		//repl.receiveReplChan = make(chan ReplicatorRequest)
		repl.replicaID = replicaID
		go repl.receiveRemoteTxns()
		go repl.replicateCycle()
	}
}

//All replicators must be added before any transaction is executed, otherwise new replicators may not receive old transactions
//I need to test this and/or read rabbitmq docs to be sure of this
func (repl *Replicator) AddRemoteReplicator(remoteID int64) {
	//repl.remoteReps = append(repl.remoteReps, remoteRepl)
	repl.remoteConn.listenToReplica(remoteID)
}

func (repl *Replicator) replicateCycle() {
	for {
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting replicateCycle")
		repl.getNewTxns()
		toSend, stableTs := repl.preparateDataToSend()
		repl.sendTxns(toSend, stableTs)
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finishing replicateCycle")
		time.Sleep(tsSendDelay * time.Millisecond)
	}
}

func (repl *Replicator) getNewTxns() {
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting getNewTxns()")
	replyChans := make([]chan StableClkUpdatesPair, len(repl.localPartitions))

	//Send request for txns
	for id, part := range repl.localPartitions {
		partEntry := repl.txnCache[id]
		replyChans[id] = make(chan StableClkUpdatesPair)
		var lastClk clocksi.Timestamp
		if len(partEntry) > 0 {
			lastClk = *partEntry[len(partEntry)-1].clk
		} else {
			lastClk = repl.lastSentClk
		}
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
		repl.txnCache[id] = append(cacheEntry, PairClockUpdates{clk: &reply.stableClock, upds: &[]UpdateObjectParams{}})
	}
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "getNewTxns() finish")
}

func (repl *Replicator) preparateDataToSend() (toSend []RemoteTxns, stableTs int64) {
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "starting prepareDataToSend")
	toSend = make([]RemoteTxns, 0, toSendInitialSize)
	foundStableClk := false
	for !foundStableClk {
		minClk := *repl.txnCache[0][0].clk //Using first entry of first partition as initial value
		var txnUpdates map[int][]UpdateObjectParams = make(map[int][]UpdateObjectParams)
		//tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "nPartitions:", len(repl.localPartitions))
		for id := 0; id < len(repl.localPartitions); id++ {
			partCache := repl.txnCache[id]

			//tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "non empty cache entry:", *partCache[0].clk, *partCache[0].upds)
			firstEntry := partCache[0]
			clkCompare := (*firstEntry.clk).Compare(minClk)
			if clkCompare == clocksi.LowerTs {
				//Clock update, this partition has no further transactions. If no smaller clk is found, then this must be the last iteration
				if len(*firstEntry.upds) == 0 {
					foundStableClk = true
				} else {
					foundStableClk = false
				}
				minClk = *firstEntry.clk
				txnUpdates = nil
				txnUpdates = make(map[int][]UpdateObjectParams)
				txnUpdates[id] = *firstEntry.upds
			} else if clkCompare == clocksi.EqualTs {
				txnUpdates[id] = *firstEntry.upds
				if len(*firstEntry.upds) == 0 {
					foundStableClk = true
				}
			}
		}
		if !foundStableClk {
			//Safe to include the actual txn's clock in the list to send. We can remove entry from cache
			tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "appending upds. Upds:", txnUpdates)
			toSend = append(toSend, RemoteTxns{Timestamp: minClk, Upds: &txnUpdates})
		} else {
			tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "storing stableClk as at least one entry was empty. Clk:", minClk.GetPos(repl.replicaID))
			stableTs = minClk.GetPos(repl.replicaID)
		}
		for id := range txnUpdates {
			repl.txnCache[id] = repl.txnCache[id][1:]
		}
	}
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finished prepareDataToSend")
	return
}

func (repl *Replicator) sendTxns(toSend []RemoteTxns, stableTs int64) {
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
/*
	repl.remoteConn.SendReplicatorRequest(&ReplicatorRequest{SenderID: repl.replicaID, Txns: toSend, StableTs: stableTs})
	tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "finished sendTxns")
}

func (repl *Replicator) receiveRemoteTxns() {
	for {
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "iterating receiveRemoteTxns")
		//remoteReq := <-repl.receiveReplChan
		remoteReq := repl.remoteConn.GetNextRemoteRequest()
		repl.tm.SendRemoteTxnRequest(TMRemoteTxn{
			ReplicaID: remoteReq.SenderID,
			Upds:      remoteReq.Txns,
			StableTs:  remoteReq.StableTs,
		})
		tools.FancyDebugPrint(tools.REPL_PRINT, repl.replicaID, "receiveRemoteTxns finished processing request")
	}
}
*/
