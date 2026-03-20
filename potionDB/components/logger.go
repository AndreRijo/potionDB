package components

import (
	fmt "fmt"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/shared/shared"

	"github.com/AndreRijo/go-tools/src/tools"
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

// Note: the returned buffer must already be clean (i.e., empty)
type LogBufferReturnArgs struct {
	Buf []PairClockUpdates
}

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
	ReturnBufLogRequest  LogRequestType = 4
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

func (args LogBufferReturnArgs) GetRequestType() (requestType LogRequestType) {
	return ReturnBufLogRequest
}

/*****In-Memory Logger implementation*****/

type MemLogger struct {
	started bool
	//log           []PairClockUpdates //TODO: Should use SliceWithCounter.
	//nextLogBuf    []PairClockUpdates //Idea: we recycle log buffers to avoid allocation/GC overload. This variable keeps a log buffer returned from Replicator that is safe to be re-used.
	log        tools.SliceWithCounter[PairClockUpdates]
	nextLogBuf tools.SliceWithCounter[PairClockUpdates] //Idea: we recycle log buffers to avoid allocation/GC overload. This variable keeps a log buffer returned from Replicator that is safe to be re-used.
	//logLock       sync.Mutex
	//matChan       chan LoggerRequest
	//replChan      chan LoggerRequest
	//lastSharedPos int
	//currentTxnPos int
	logChan chan LoggerRequest
	partId  uint64
	mat     *Materializer //Used to send the safeClk request

	replReplyChan chan StableClkUpdatesPair
	//timer          *time.Timer
	matTimeoutChan chan bool //Check matTimeoutHelper() for details
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
	logger.logChan <- request
}

// Starts goroutine that listens to requests
func (logger *MemLogger) Initialize(mat *Materializer, partId uint64) {
	if !logger.started {
		logger.log = tools.NewSliceWithCounter[PairClockUpdates](initLogCapacity)
		logger.logChan = make(chan LoggerRequest, 500)
		//logger.logChan = make(chan LoggerRequest, 10000)
		logger.started = true
		logger.partId = partId
		logger.mat = mat
		//logger.matReplyChan = make(chan clocksi.Timestamp, 5) //Mat may reply late. If so, we will ignore previous reply.
		go logger.handleRequests()
		//go logger.forceClean()
		go logger.matTimeoutHelper()
	}
}

func (logger *MemLogger) Reset() {
	//logger.log = make([]PairClockUpdates, 0, initLogCapacity)
	//logger.lastSharedPos, logger.currentTxnPos = 0, 0
	logger.log = tools.NewSliceWithCounter[PairClockUpdates](initLogCapacity)
	fmt.Printf("[LOG %d]Reset complete.\n", logger.partId)
}

func (logger *MemLogger) handleRequests() {
	for {
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
		case ReturnBufLogRequest:
			logger.handleBufferReturnRequest(req.LogRequestArgs.(LogBufferReturnArgs))
		default:
			fmt.Printf("[LOG%d]Unexpected request: %+v\n", logger.partId, req)
		}
	}
}

func (logger *MemLogger) handleCommitLogRequest(request LogCommitArgs) {
	if shared.IsLogDisabled {
		return
	}
	//fmt.Printf("[LOG%d]Appending txn to log with clk %s and %d upds.\n", logger.partId, request.TxnClk.ToString(), len(request.Upds))
	logger.log.Append(PairClockUpdates{clk: request.TxnClk, upds: request.Upds})
	/*if logger.currentTxnPos == cap(logger.log) {
		logger.log = append(logger.log, PairClockUpdates{clk: request.TxnClk, upds: request.Upds})
		logger.log = logger.log[:cap(logger.log)]
	} else {
		logger.log[logger.currentTxnPos] = PairClockUpdates{clk: request.TxnClk, upds: request.Upds}
	}
	logger.currentTxnPos++*/
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
		request.ReplyChan <- StableClkUpdatesPair{stableClock: clocksi.NewSliceTimestamp(), upds: []PairClockUpdates{}, partID: logger.partId}
		return
	}
	logger.replReplyChan = request.ReplyChan
	//fmt.Printf("[LOG%d]Received txn log request. Sending safe clock request to MAT. Current time: %v\n", logger.partId, time.Now().Format("15:04:05.000"))
	//Send on a different goroutine to prevent log from blocking if materializer's channel is full.
	go logger.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatSafeClkArgs{}}, uint64(logger.partId))
}

func (logger *MemLogger) handleMatClkRequest(request LogClkArgs) {
	//logger.timer.Stop()
	if logger.replReplyChan == nil {
		//fmt.Printf("[LOG%d]Received clk from MAT, but the timeout already fired and replied. Thus, ignoring MAT reply. Current time: %v\n", logger.partId, time.Now().Format("15:04:05.000"))
		return
	}
	/*var txns []PairClockUpdates
	if logger.lastSharedPos == logger.currentTxnPos {
		txns = nil
	} else {
		txns = logger.log[logger.lastSharedPos:logger.currentTxnPos]
		logger.log, logger.lastSharedPos, logger.currentTxnPos = make([]PairClockUpdates, len(logger.log)), 0, 0
		//We create a new log as we cannot re-use the positions we have shared with the Replicator - we don't know when Replicator will be done with copying them.
		//So, unless we want to wait for Replicator to be done copying (bad idea as we would need to sync with all partitions in practice), better do like this
		//At least this way the data will get automatically GC'ed.
	}
	//fmt.Printf("[LOG%d]Received clk from MAT. Sending last clock of log to repl. NTxns: %d. Current time: %v\n", logger.partId, len(txns), time.Now().Format("15:04:05.000"))
	//fmt.Printf("Log%d to Repl: %d txns.\t", logger.partId, len(txns))
	//logger.replReplyChan <- StableClkUpdatesPair{stableClock: request.Clk, upds: txns, partID: logger.partId}
	//logger.replReplyChan = nil*/

	logger.replyReplHelper(request.Clk)
}

func (logger *MemLogger) handleClkTimeoutRequest() {
	//if logger.replReplyChan == nil || logger.lastSharedPos == logger.currentTxnPos {
	if logger.replReplyChan == nil || logger.log.IsEmpty() {
		//fmt.Printf("[LOG%d]Timer fired. Is ReplReplyChan nil? %v. Is lastSharedPos == currentTxnPos? %v. Current time: %v.\n",
		//logger.partId, logger.replReplyChan == nil, logger.lastSharedPos == logger.currentTxnPos, time.Now().Format("15:04:05.000"))
		return //If the former, we already received the reply from MAT and replied to Repl. If the later, we have to keep waiting for MAT :( (as there are no txns, so we need mat to give us a clk to know what's safe))
	}

	logger.replyReplHelper(logger.log.Get(logger.log.Len() - 1).clk.Copy())

	/*txns := logger.log[logger.lastSharedPos:logger.currentTxnPos]
	logger.log, logger.lastSharedPos, logger.currentTxnPos = make([]PairClockUpdates, len(logger.log)), 0, 0
	//Check comments on handleMatClkRequest.

	//fmt.Printf("[LOG%d]Timer fired. Sending last clock of log to repl. Current time: %v.\n", logger.partId, time.Now().Format("15:04:05.000"))
	logger.replReplyChan <- StableClkUpdatesPair{stableClock: txns[len(txns)-1].clk.Copy(), upds: txns, partID: logger.partId}
	logger.replReplyChan = nil*/
}

func (logger *MemLogger) replyReplHelper(stableClk clocksi.Timestamp) {
	txns := logger.log.ToSlice()
	//fmt.Printf("[LOG%d]Replying to repl with stable clock %s and %d txns.\n", logger.partId, stableClk.ToString(), len(txns))
	logger.replReplyChan <- StableClkUpdatesPair{stableClock: stableClk, upds: txns, partID: logger.partId}
	logger.replReplyChan = nil
	if logger.nextLogBuf.Cap() > 0 { //We can re-use this buffer
		logger.log = logger.nextLogBuf
		logger.nextLogBuf = tools.SliceWithCounter[PairClockUpdates]{}
	} else {
		logger.log = tools.NewSliceWithCounter[PairClockUpdates](tools.Max(len(txns), initLogCapacity))
	}
}

// This routines waits for handleTxnLogRequest() to be fired. It prepares a timeout in case Materializer is busy.
func (logger *MemLogger) matTimeoutHelper() {
	for {
		<-logger.matTimeoutChan //Wait for a message to arrive.
		select {
		case <-logger.matTimeoutChan: //If we receive this, it means the timeout did not fire. Good! Nothing to do.
		case <-time.After(500 * time.Millisecond): //If we receive this, it means the timeout fired.
			logger.logChan <- LoggerRequest{LogClkTimeoutArgs{}}
		}
	}
}

func (logger *MemLogger) handleBufferReturnRequest(request LogBufferReturnArgs) {
	if logger.nextLogBuf.Cap() > cap(request.Buf) { //In case we already have a buffer, we keep the longest one.
		logger.nextLogBuf = tools.ToSliceWithCounter(request.Buf[:cap(request.Buf)]) //Unlock full capacity.
	} //else: just ignore. Later GC will get rid of it.
}

// NOTE: THIS IS TEMPORARY. THIS WILL LEAD TO TROUBLE, AS IT MAY REMOVE ENTRIES THAT REPL MAY STILL REQUEST OR IS STILL USING.
func (logger *MemLogger) forceClean() {
	for {
		time.Sleep(140 * time.Second)
		/*for i := range logger.log {
			logger.log[i] = PairClockUpdates{}
		}*/
		logger.log.DeepClear()
		fmt.Printf("[LOG %d]Forced log clear.\n", logger.partId)
	}
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
