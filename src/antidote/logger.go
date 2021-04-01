package antidote

import (
	"potionDB/src/clocksi"
	fmt "fmt"
	"potionDB/src/shared"
	"sync"
)

/*****Logging interface*****/

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
	TxnClk *clocksi.Timestamp
	Upds   []*UpdateObjectParams //Should be downstream arguments
}

type LogTxnArgs struct {
	lastClock clocksi.Timestamp
	ReplyChan chan StableClkUpdatesPair
}

type StableClkUpdatesPair struct {
	upds        []PairClockUpdates
	stableClock clocksi.Timestamp
}

type LogRequestType byte

type BoolTimestampPair struct {
	bool
	clocksi.Timestamp
}

const (
	//Types of requests
	CommitLogRequest LogRequestType = 0
	TxnLogRequest    LogRequestType = 1
)

func (args LogCommitArgs) GetRequestType() (requestType LogRequestType) {
	return CommitLogRequest
}

func (args LogTxnArgs) GetRequestType() (requestType LogRequestType) {
	return TxnLogRequest
}

/*****In-Memory Logger implementation*****/

type MemLogger struct {
	started    bool
	log        []PairClockUpdates
	currLogPos int
	matChan    chan LoggerRequest
	replChan   chan LoggerRequest
	partId     uint64
	mat        *Materializer //Used to send the safeClk request
	//Protects log. While we want to process materializer/replicator requests concurrently
	//(in order to avoid each one waiting for the other and, thus, block forever), we want
	//to isolate access to the log.
	logLock sync.Mutex
}

type PairClockUpdates struct {
	clk  *clocksi.Timestamp
	upds []*UpdateObjectParams
}

const (
	initLogCapacity = 100
	keepWholeLog    = false
)

/*
func (logger *MemLogger) SendLoggerRequest(request LoggerRequest) {
	logger.sendChan <- request
}
*/

func (logger *MemLogger) SendLoggerRequest(request LoggerRequest) {
	switch request.GetRequestType() {
	case CommitLogRequest:
		logger.matChan <- request
	case TxnLogRequest:
		logger.replChan <- request
	}
}

//Starts goroutine that listens to requests
func (logger *MemLogger) Initialize(mat *Materializer, partId uint64) {
	if !logger.started {
		logger.log = make([]PairClockUpdates, 0, initLogCapacity)
		logger.currLogPos = 0
		logger.matChan = make(chan LoggerRequest)
		logger.replChan = make(chan LoggerRequest)
		logger.started = true
		logger.partId = partId
		logger.mat = mat
		go logger.handleMatRequests()
		go logger.handleReplicatorRequests()
	}
}

func (logger *MemLogger) Reset() {
	logger.log = make([]PairClockUpdates, 0, initLogCapacity)
	logger.currLogPos = 0
	fmt.Printf("[LOG %d]Reset complete.\n", logger.partId)
}

/*
func (logger *MemLogger) handleRequests() {
	for {
		request := <-logger.sendChan
		switch request.GetRequestType() {
		case CommitLogRequest:
			logger.handleCommitLogRequest(request.LogRequestArgs.(LogCommitArgs))
		case TxnLogRequest:
			logger.handleTxnLogRequest(request.LogRequestArgs.(LogTxnArgs))
		}
	}
}
*/
func (logger *MemLogger) handleMatRequests() {
	for {
		if shared.IsLogDisabled {
			<-logger.matChan
		} else {
			request := <-logger.matChan
			logger.handleCommitLogRequest(request.LogRequestArgs.(LogCommitArgs))
		}
	}
}

func (logger *MemLogger) handleReplicatorRequests() {
	for {
		request := <-logger.replChan
		logger.handleTxnLogRequest(request.LogRequestArgs.(LogTxnArgs))
	}
}

func (logger *MemLogger) handleCommitLogRequest(request LogCommitArgs) {
	logger.logLock.Lock()
	logger.log = append(logger.log, PairClockUpdates{clk: request.TxnClk, upds: request.Upds})
	logger.logLock.Unlock()
}

func (logger *MemLogger) handleTxnLogRequest(request LogTxnArgs) {
	if shared.IsLogDisabled {
		request.ReplyChan <- StableClkUpdatesPair{stableClock: clocksi.NewClockSiTimestampFromId(0), upds: []PairClockUpdates{}}
		return
	}
	var txns []PairClockUpdates
	logger.logLock.Lock()
	if logger.currLogPos == len(logger.log) {
		txns = nil
	} else {
		txns = logger.log[logger.currLogPos:]
		logger.currLogPos = len(logger.log)
		if !keepWholeLog {
			logger.log = make([]PairClockUpdates, 0, initLogCapacity)
			logger.currLogPos = 0
		}
	}
	logger.logLock.Unlock()
	replyChan := make(chan clocksi.Timestamp)
	logger.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatSafeClkArgs{ReplyChan: replyChan}}, uint64(logger.partId))
	stableClk := <-replyChan
	request.ReplyChan <- StableClkUpdatesPair{stableClock: stableClk, upds: txns}
}
