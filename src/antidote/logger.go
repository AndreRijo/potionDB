package antidote

import (
	"clocksi"
)

/*****Logging interface*****/

type Logger interface {
	//Initializes the logger belogging to partition partitionID. Must be called before any other method of the Logger.
	Initialize(mat *Materializer, partitionID uint64)
	//Send a request to the logger.
	//Each logger implementation must support the following requests: LogCommitArgs, LogNextClkArgs
	SendLoggerRequest(request LoggerRequest)
}

type LoggerRequest struct {
	LogRequestArgs
}

type LogRequestArgs interface {
	GetRequestType() (requestType LogRequestType)
}

type LogCommitArgs struct {
	TxnClk *clocksi.Timestamp
	Upds   *[]UpdateObjectParams //Should be downstream arguments
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
	sendChan   chan LoggerRequest
	partId     uint64
	mat        *Materializer //Used to send the safeClk request
}

type PairClockUpdates struct {
	clk  *clocksi.Timestamp
	upds *[]UpdateObjectParams
}

const (
	initLogCapacity = 100
)

func (logger *MemLogger) SendLoggerRequest(request LoggerRequest) {
	logger.sendChan <- request
}

//Starts goroutine that listens to requests
func (logger *MemLogger) Initialize(mat *Materializer, partId uint64) {
	if !logger.started {
		logger.log = make([]PairClockUpdates, 0, initLogCapacity)
		logger.currLogPos = 0
		logger.sendChan = make(chan LoggerRequest)
		logger.started = true
		logger.partId = partId
		logger.mat = mat
		go logger.handleRequests()
	}
}

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

func (logger *MemLogger) handleCommitLogRequest(request LogCommitArgs) {
	logger.log = append(logger.log, PairClockUpdates{clk: request.TxnClk, upds: request.Upds})
}

func (logger *MemLogger) handleTxnLogRequest(request LogTxnArgs) {
	var txns []PairClockUpdates
	if logger.currLogPos == len(logger.log) {
		txns = nil
	} else {
		txns = logger.log[logger.currLogPos:]
		logger.currLogPos = len(logger.log)
	}
	replyChan := make(chan clocksi.Timestamp)
	logger.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatSafeClkArgs{ReplyChan: replyChan}}, uint64(logger.partId))
	stableClk := <-replyChan
	request.ReplyChan <- StableClkUpdatesPair{stableClock: stableClk, upds: txns}

}
