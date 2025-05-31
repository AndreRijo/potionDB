package components

import (
	fmt "fmt"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/shared/shared"
)

/*****Logging interface*****/
//TODO: Improve with a switch of channels (or select?). Better than use two routines with locks I think.
//TODO: Detect when there aren't updates being done. In that case, can deep clean the log.

type Logger interface {
	//Initializes the logger belogging to partition partitionID. Must be called before any other method of the Logger.
	Initialize(mat *Materializer, partitionID uint64)
	//Send a request to the logger.
	//Each logger implementation must support the following requests: LogCommitArgs, LogNextClkArgs
	SendLoggerRequest(request LoggerRequest)
	//Resets the status of the logger to the initial state (i.e., empty log)
	Reset()
}

type LoggerRequest struct {
	LogRequestArgs
}

type LogRequestArgs interface {
	GetRequestType() (requestType LogRequestType)
}

type LogCommitArgs struct {
	TxnClk clocksi.Timestamp
	Upds   []crdt.UpdateObjectParams //Should be downstream arguments
}

type LogTxnArgs struct {
	lastClock clocksi.Timestamp
	ReplyChan chan StableClkUpdatesPair
}

type LogClkArgs struct {
	Clk clocksi.Timestamp
}

type LogClkTimeoutArgs struct{}

type StableClkUpdatesPair struct {
	upds        []PairClockUpdates
	stableClock clocksi.Timestamp
	partID      uint64
}

type LogRequestType byte

type BoolTimestampPair struct {
	bool
	clocksi.Timestamp
}

const (
	//Types of requests
	CommitLogRequest     LogRequestType = 0
	TxnLogRequest        LogRequestType = 1
	ClkLogRequest        LogRequestType = 2
	LogClkTimeoutRequest LogRequestType = 3
)

func (args LogCommitArgs) GetRequestType() (requestType LogRequestType) {
	return CommitLogRequest
}

func (args LogTxnArgs) GetRequestType() (requestType LogRequestType) {
	return TxnLogRequest
}

func (args LogClkArgs) GetRequestType() (requestType LogRequestType) {
	return ClkLogRequest
}

func (args LogClkTimeoutArgs) GetRequestType() (requestType LogRequestType) {
	return LogClkTimeoutRequest
}

/*****In-Memory Logger implementation*****/

type MemLogger struct {
	started       bool
	log           []PairClockUpdates
	lastSharedPos int
	currentTxnPos int
	//matChan       chan LoggerRequest
	//replChan      chan LoggerRequest
	logChan chan LoggerRequest
	partId  uint64
	mat     *Materializer //Used to send the safeClk request
	//Protects log. While we want to process materializer/replicator requests concurrently
	//(in order to avoid each one waiting for the other and, thus, block forever), we want
	//to isolate access to the log.
	//logLock sync.Mutex
	//matReplyChan  chan clocksi.Timestamp

	replReplyChan chan StableClkUpdatesPair
	timer         *time.Timer
}

type PairClockUpdates struct {
	clk  clocksi.Timestamp
	upds []crdt.UpdateObjectParams
}

const (
	initLogCapacity = 1000
	keepWholeLog    = false
)

func (logger *MemLogger) SendLoggerRequest(request LoggerRequest) {
	/*timer := time.NewTimer(7 * time.Second)
	select {
	case logger.logChan <- request:
		timer.Stop()
	case <-timer.C:
		fmt.Printf("[LOG%d]Timeout while sending request of type %v to logger %d.\n", logger.partId, request.GetRequestType(), logger.partId)
	}*/
	logger.logChan <- request
}

/*
func (logger *MemLogger) SendLoggerRequest(request LoggerRequest) {
	switch request.GetRequestType() {
	case CommitLogRequest:
		logger.matChan <- request
	case TxnLogRequest:
		logger.replChan <- request
	}
}
*/

// Starts goroutine that listens to requests
func (logger *MemLogger) Initialize(mat *Materializer, partId uint64) {
	if !logger.started {
		logger.log = make([]PairClockUpdates, initLogCapacity)
		logger.lastSharedPos, logger.currentTxnPos = 0, 0
		//logger.matChan = make(chan LoggerRequest, 1)
		//logger.replChan = make(chan LoggerRequest, 1)
		logger.logChan = make(chan LoggerRequest, 500)
		//logger.logChan = make(chan LoggerRequest, 10000)
		logger.started = true
		logger.partId = partId
		logger.mat = mat
		//logger.matReplyChan = make(chan clocksi.Timestamp, 5) //Mat may reply late. If so, we will ignore previous reply.
		go logger.handleRequests()
		//go logger.handleMatRequests()
		//go logger.handleReplicatorRequests()
	}
}

func (logger *MemLogger) Reset() {
	//logger.log = make([]PairClockUpdates, 0, initLogCapacity)
	logger.log = make([]PairClockUpdates, initLogCapacity)
	logger.lastSharedPos, logger.currentTxnPos = 0, 0
	fmt.Printf("[LOG %d]Reset complete.\n", logger.partId)
}

func (logger *MemLogger) handleRequests() {
	//var timer *time.Timer
	for {
		/*timer = time.NewTimer(7 * time.Second)
		select {
		case req := <-logger.logChan:
			timer.Stop()
			switch req.GetRequestType() {
			case TxnLogRequest:
				logger.handleTxnLogRequest(req.LogRequestArgs.(LogTxnArgs))
			case CommitLogRequest:
				logger.handleCommitLogRequest(req.LogRequestArgs.(LogCommitArgs))
			case ClkLogRequest:
				logger.handleMatClkRequest(req.LogRequestArgs.(LogClkArgs))
			case LogClkTimeoutRequest:
				logger.handleClkTimeoutRequest()
			default:
				fmt.Println("[LOG]Unexpected request:", req)
			}
		case <-timer.C:
			fmt.Printf("[LOG%d]No request received in the last 7s. Current time: %v.\n", logger.partId, time.Now().Format("15:04:05.000"))
		}*/
		req := <-logger.logChan
		//fmt.Printf("[LOG%d]Got request %d at %s.\n", logger.partId, req.GetRequestType(), time.Now().Format("15:04:05.000"))
		switch req.GetRequestType() {
		case TxnLogRequest:
			logger.handleTxnLogRequest(req.LogRequestArgs.(LogTxnArgs))
		case CommitLogRequest:
			logger.handleCommitLogRequest(req.LogRequestArgs.(LogCommitArgs))
		case ClkLogRequest:
			logger.handleMatClkRequest(req.LogRequestArgs.(LogClkArgs))
		case LogClkTimeoutRequest:
			logger.handleClkTimeoutRequest()
		default:
			fmt.Println("[LOG]Unexpected request:", req)
		}
	}
}

/*
func (logger *MemLogger) handleMatRequests() {
	for {
		if shared.IsLogDisabled {
			<-logger.matChan
		} else {
			request := <-logger.matChan
			logger.handleCommitLogRequest(request.LogRequestArgs.(LogCommitArgs))
		}
	}
}*/

/*func (logger *MemLogger) handleReplicatorRequests() {
	for {
		request := <-logger.replChan
		logger.handleTxnLogRequest(request.LogRequestArgs.(LogTxnArgs))
	}
}*/

func (logger *MemLogger) handleCommitLogRequest(request LogCommitArgs) {
	//logger.logLock.Lock()
	if shared.IsLogDisabled {
		return
	}
	if logger.currentTxnPos == cap(logger.log) {
		logger.log = append(logger.log, PairClockUpdates{clk: request.TxnClk, upds: request.Upds})
		logger.log = logger.log[:cap(logger.log)]
	} else {
		logger.log[logger.currentTxnPos] = PairClockUpdates{clk: request.TxnClk, upds: request.Upds}
	}
	logger.currentTxnPos++
	//logger.logLock.Unlock()
}

/*
In order to avoid deadlocks with channels (as we have MAT -> LOG and LOG -> MAT communication), the following is done:
- First, on txnLogRequest arrival, we send a request to MAT for the safe clock.
- Then, we go back to processing requests.
- Eventually, mat will reply (through logger.logChan) with a LogClkArgs request
- We now finish the processing of the txnLogRequest. As a benefit of this method, we send a more up-to-date log! And no deadlocks!
- As an ending detail, we set a timeout, as MAT may be busy with a long txn. In that case, we use the last clock of the log.
*/
func (logger *MemLogger) handleTxnLogRequest(request LogTxnArgs) {
	if shared.IsLogDisabled {
		request.ReplyChan <- StableClkUpdatesPair{stableClock: clocksi.NewClockSiTimestampFromId(0), upds: []PairClockUpdates{}, partID: logger.partId}
		return
	}
	/*if logger.currentTxnPos > 0 {
		request.ReplyChan <- StableClkUpdatesPair{stableClock: logger.log[logger.currentTxnPos-1].clk.Copy(), upds: nil, partID: logger.partId}
		return
	}
	request.ReplyChan <- StableClkUpdatesPair{stableClock: clocksi.DummyTs.Copy(), upds: nil, partID: logger.partId}
	return*/
	logger.replReplyChan = request.ReplyChan
	logger.timer = time.NewTimer(500 * time.Millisecond)
	go func() { //TODO: Possible goroutine leak... Would be nice to improve on this. Maybe should make an always ongoing goroutine for doing this task?
		//To avoid concurrency issues, we send a request to... the log.
		<-logger.timer.C
		logger.logChan <- LoggerRequest{LogClkTimeoutArgs{}}
	}()
	//fmt.Printf("[LOG%d]Received txn log request. Sending safe clock request to MAT. Current time: %v\n", logger.partId, time.Now().Format("15:04:05.000"))
	//Send on a different goroutine to prevent log from blocking if materializer's channel is full.
	go logger.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatSafeClkArgs{}}, uint64(logger.partId))
}

func (logger *MemLogger) handleMatClkRequest(request LogClkArgs) {
	logger.timer.Stop()
	if logger.replReplyChan == nil {
		//fmt.Printf("[LOG%d]Received clk from MAT, but the timeout already fired and replied. Thus, ignoring MAT reply. Current time: %v\n", logger.partId, time.Now().Format("15:04:05.000"))
		return
	}
	var txns []PairClockUpdates
	if logger.lastSharedPos == logger.currentTxnPos {
		txns = nil
	} else {
		txns = logger.log[logger.lastSharedPos:logger.currentTxnPos]
		logger.lastSharedPos = logger.currentTxnPos
		if !keepWholeLog {
			logger.lastSharedPos, logger.currentTxnPos = 0, 0
		}
	}
	//fmt.Printf("[LOG%d]Received clk from MAT. Sending last clock of log to repl. Current time: %v\n", logger.partId, time.Now().Format("15:04:05.000"))
	logger.replReplyChan <- StableClkUpdatesPair{stableClock: request.Clk, upds: txns, partID: logger.partId}
	logger.replReplyChan = nil
}

func (logger *MemLogger) handleClkTimeoutRequest() {
	if logger.replReplyChan == nil || logger.lastSharedPos == logger.currentTxnPos {
		//fmt.Printf("[LOG%d]Timer fired. Is ReplReplyChan nil? %v. Is lastSharedPost == currentTxnPos? %v. Current time: %v.\n",
		//logger.partId, logger.replReplyChan == nil, logger.lastSharedPos == logger.currentTxnPos, time.Now().Format("15:04:05.000"))
		return //If the former, we already received the reply from MAT and replied to Repl. If the later, we have to keep waiting for MAT :()
	}

	txns := logger.log[logger.lastSharedPos:logger.currentTxnPos]
	logger.lastSharedPos = logger.currentTxnPos
	if !keepWholeLog {
		logger.lastSharedPos, logger.currentTxnPos = 0, 0
	}

	//fmt.Printf("[LOG%d]Timer fired. Sending last clock of log to repl. Current time: %v.\n", logger.partId, time.Now().Format("15:04:05.000"))
	logger.replReplyChan <- StableClkUpdatesPair{stableClock: txns[len(txns)-1].clk.Copy(), upds: txns, partID: logger.partId}
	logger.replReplyChan = nil
}

/*
func (logger *MemLogger) handleTxnLogRequest(request LogTxnArgs) {
	if shared.IsLogDisabled {
		request.ReplyChan <- StableClkUpdatesPair{stableClock: clocksi.NewClockSiTimestampFromId(0), upds: []PairClockUpdates{}, partID: logger.partId}
		return
	}
	for len(logger.matReplyChan) > 0 { //From a previous clk request for which the partition did not reply on time. Skip it.
		<-logger.matReplyChan
	}
	var txns []PairClockUpdates
	//logger.logLock.Lock()
	if logger.lastSharedPos == logger.currentTxnPos {
		txns = nil
	} else {
		txns = logger.log[logger.lastSharedPos:logger.currentTxnPos]
		logger.lastSharedPos = logger.currentTxnPos
		if !keepWholeLog {
			//logger.log = make([]PairClockUpdates, 0, initLogCapacity)
			logger.lastSharedPos, logger.currentTxnPos = 0, 0
		}
	}
	//logger.logLock.Unlock()
	logger.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatSafeClkArgs{ReplyChan: logger.matReplyChan}}, uint64(logger.partId))
	var stableClk clocksi.Timestamp
	if txns == nil { //Must wait for the reply
		stableClk = <-logger.matReplyChan
	} else { //Wait only for a short while as we have a clock from txns.
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case stableClk = <-logger.matReplyChan:
			timer.Stop()
		case <-timer.C: //Timeout, use last txn clock
			fmt.Printf("[LOG][Part%d]Materializer of partition %d seems to be busy. Using clk of last commited txn for replication.\n", logger.partId, logger.partId)
			stableClk = txns[len(txns)-1].clk.Copy()
		}
	}
	request.ReplyChan <- StableClkUpdatesPair{stableClock: stableClk, upds: txns, partID: logger.partId}
}
*/
