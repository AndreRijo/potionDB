package antidote

import (
	"clocksi"
	fmt "fmt"
	"time"
)

type Replicator struct {
	tm              *TransactionManager //to send request to downstream transactions
	localPartitions []Logger
	remoteReps      []chan ReplicatorRequest
	txnCache        map[int][]PairClockUpdates //int: partitionID
	lastSentClk     clocksi.Timestamp
	receiveReplChan chan ReplicatorRequest
	started         bool
	replicaID       int64
}

type ReplicatorRequest struct {
	senderID int64
	txns     []RemoteTxns
	stableTs int64
}

type RemoteTxns struct {
	clocksi.Timestamp
	upds *map[int][]UpdateObjectParams
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
		repl.remoteReps = make([]chan ReplicatorRequest, 0)
		repl.txnCache = make(map[int][]PairClockUpdates)
		for id := 0; id < int(nGoRoutines); id++ {
			repl.txnCache[id] = make([]PairClockUpdates, 0, cacheInitialSize)
		}
		repl.lastSentClk = clocksi.NewClockSiTimestamp()
		repl.receiveReplChan = make(chan ReplicatorRequest)
		repl.replicaID = replicaID
		go repl.receiveRemoteTxns()
		go repl.replicateCycle()
	}
}

//TODO: TEMPORARY METHOD - find a decent way to add remote replicators. This has concurrency issues with replicateCycle()
//All replicators must be added before any transaction is executed, otherwise new replicators will never receive old transactions
func (repl *Replicator) tmpAddRemoteReplicator(remoteRepl chan ReplicatorRequest) {
	repl.remoteReps = append(repl.remoteReps, remoteRepl)
}

func (repl *Replicator) replicateCycle() {
	for {
		fmt.Println("[Replicator", repl.replicaID, "]starting replicateCycle")
		repl.getNewTxns()
		toSend, stableTs := repl.preparateDataToSend()
		repl.sendTxns(toSend, stableTs)
		fmt.Println("[Replicator", repl.replicaID, "]finishing replicateCycle")
		time.Sleep(tsSendDelay * time.Millisecond)
	}
}

func (repl *Replicator) getNewTxns() {
	fmt.Println("[Replicator", repl.replicaID, "]starting getNewTxns()")
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
	fmt.Println("[Replicator", repl.replicaID, "]getNewTxns() second part")

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
	fmt.Println("[Replicator", repl.replicaID, "]getNewTxns() finish")
}

func (repl *Replicator) preparateDataToSend() (toSend []RemoteTxns, stableTs int64) {
	fmt.Println("[Replicator", repl.replicaID, "]starting prepareDataToSend")
	toSend = make([]RemoteTxns, 0, toSendInitialSize)
	foundStableClk := false
	for !foundStableClk {
		minClk := *repl.txnCache[0][0].clk //Using first entry of first replica as initial value
		var txnUpdates map[int][]UpdateObjectParams = make(map[int][]UpdateObjectParams)
		fmt.Println("[Replicator", repl.replicaID, "]nPartitions:", len(repl.localPartitions))
		for id := 0; id < len(repl.localPartitions); id++ {
			partCache := repl.txnCache[id]

			fmt.Println("[Replicator", repl.replicaID, "]non empty cache entry:", *partCache[0].clk, *partCache[0].upds)
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
			}
		}
		if !foundStableClk {
			//Safe to include the actual txn's clock in the list to send. We can remove entry from cache
			fmt.Println("[Replicator", repl.replicaID, "]appending upds. Upds:", txnUpdates)
			toSend = append(toSend, RemoteTxns{Timestamp: minClk, upds: &txnUpdates})
		} else {
			fmt.Println("[Replicator", repl.replicaID, "]storing stableClk as at least one entry was empty. Clk:", minClk.GetPos(repl.replicaID))
			stableTs = minClk.GetPos(repl.replicaID)
		}
		for id := range txnUpdates {
			repl.txnCache[id] = repl.txnCache[id][1:]
		}
	}
	fmt.Println("[Replicator", repl.replicaID, "]finished prepareDataToSend")
	return
}

func (repl *Replicator) sendTxns(toSend []RemoteTxns, stableTs int64) {
	fmt.Print("[Replicator", repl.replicaID, "]starting sendTxns:[")
	for _, txn := range toSend {
		fmt.Print("{", txn.Timestamp, "|", *txn.upds, "},")
	}
	fmt.Println("]")
	for _, remoteChan := range repl.remoteReps {
		remoteChan <- ReplicatorRequest{senderID: repl.replicaID, txns: toSend, stableTs: stableTs}
	}
	fmt.Println("[Replicator", repl.replicaID, "]finished sendTxns")
}

func (repl *Replicator) receiveRemoteTxns() {
	for {
		fmt.Println("[Replicator", repl.replicaID, "]iterating receiveRemoteTxns")
		remoteReq := <-repl.receiveReplChan
		repl.tm.SendRemoteTxnRequest(TMRemoteTxn{
			ReplicaID: remoteReq.senderID,
			Upds:      remoteReq.txns,
			StableTs:  remoteReq.stableTs,
		})
		fmt.Println("[Replicator", repl.replicaID, "]receiveRemoteTxns finished processing request")
	}
}
