package antidote

import (
	"clocksi"
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
}

type ReplicatorRequest struct {
	senderID int64
	txns     []RemoteTxns
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

func (repl *Replicator) Initialize(tm *TransactionManager, loggers []Logger) {
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
		go repl.receiveRemoteTxns()
		go repl.replicateCycle()
	}
}

func (repl *Replicator) replicateCycle() {
	for {
		repl.getNewTxns()
		toSend := repl.preparateDataToSend()
		repl.sendTxns(toSend)
		time.Sleep(tsSendDelay * time.Millisecond)
	}
}

func (repl *Replicator) getNewTxns() {
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

	//Receive replies and cache them
	for id, replyChan := range replyChans {
		reply := <-replyChan
		cacheEntry := repl.txnCache[id]
		//Remove last entry which is the previous, old, stable block
		if len(cacheEntry) > 0 {
			cacheEntry = cacheEntry[:len(cacheEntry)-1]
		}
		cacheEntry = append(cacheEntry, reply.upds...)
		repl.txnCache[id] = append(cacheEntry, PairClockUpdates{clk: &reply.stableClock, upds: &[]UpdateObjectParams{}})
	}
}

func (repl *Replicator) preparateDataToSend() (toSend []RemoteTxns) {
	toSend = make([]RemoteTxns, 0, toSendInitialSize)
	foundEmpty := false
	for !foundEmpty {
		minClk := clocksi.DummyHighTs
		var txnUpdates map[int][]UpdateObjectParams = nil
		for id := 0; id < len(repl.localPartitions) && !foundEmpty; id++ {
			partCache := repl.txnCache[id]
			//The clock of this partition is behind the remaining noes, so we can't send more txns.
			if len(partCache) == 0 {
				foundEmpty = true
			} else {
				firstEntry := partCache[0]
				clkCompare := (*firstEntry.clk).Compare(minClk)
				if clkCompare == clocksi.LowerTs {
					minClk = *firstEntry.clk
					txnUpdates = nil
					txnUpdates = make(map[int][]UpdateObjectParams)
					txnUpdates[id] = *firstEntry.upds
				} else if clkCompare == clocksi.EqualTs {
					txnUpdates[id] = *firstEntry.upds
				}
			}
		}
		//Safe to include the actual txn's clock in the list to send. We can remove entry from cache
		if !foundEmpty {
			toSend = append(toSend, RemoteTxns{Timestamp: minClk, upds: &txnUpdates})
			for id := range txnUpdates {
				repl.txnCache[id] = repl.txnCache[id][1:]
			}
		}
	}
	return
}

func (repl *Replicator) sendTxns(toSend []RemoteTxns) {
	for _, remoteChan := range repl.remoteReps {
		remoteChan <- ReplicatorRequest{senderID: ReplicaID, txns: toSend}
	}
}

func (repl *Replicator) receiveRemoteTxns() {
	for {
		remoteReq := <-repl.receiveReplChan
		repl.tm.SendRemoteTxnRequest(TMRemoteTxn{
			ReplicaID: remoteReq.senderID,
			Upds:      remoteReq.txns,
		})
	}
}
