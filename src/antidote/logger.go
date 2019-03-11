package antidote

import (
	"clocksi"
)

//TODO: Delete methods with string "old" in their name

/*****Logging interface*****/

type Logger interface {
	//Initializes the logger belogging to partition partitionID. Must be called before any other method of the Logger.
	Initialize(partitionID uint64)
	//Send a request to the logger.
	//Each logger implementation must support the following requests: LogCommitArgs, LogNextClkArgs, LogOldTxnArgs
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

type LogOldNextClkArgs struct {
	PreviousClk clocksi.Timestamp
	ReplyChan   chan BoolTimestampPair
}

type LogOldTxnArgs struct {
	TxnClk    clocksi.Timestamp
	ReplyChan chan *[]UpdateObjectParams
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
	CommitLogRequest     LogRequestType = 0
	TxnLogRequest        LogRequestType = 1
	NextClkOldLogRequest LogRequestType = 2
	TxnOldLogRequest     LogRequestType = 3
)

func (args LogCommitArgs) GetRequestType() (requestType LogRequestType) {
	return CommitLogRequest
}

func (args LogOldNextClkArgs) GetRequestType() (requestType LogRequestType) {
	return NextClkOldLogRequest
}

func (args LogOldTxnArgs) GetRequestType() (requestType LogRequestType) {
	return TxnOldLogRequest
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
func (logger *MemLogger) Initialize(partId uint64) {
	if !logger.started {
		logger.log = make([]PairClockUpdates, 0, initLogCapacity)
		logger.currLogPos = 0
		logger.sendChan = make(chan LoggerRequest)
		logger.started = true
		logger.partId = partId
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
		case NextClkOldLogRequest:
			logger.handleNextClkOldLogRequest(request.LogRequestArgs.(LogOldNextClkArgs))
		case TxnOldLogRequest:
			logger.handleTxnOldLogRequest(request.LogRequestArgs.(LogOldTxnArgs))
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
	SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatSafeClkArgs{ReplyChan: replyChan}}, uint64(logger.partId))
	stableClk := <-replyChan
	request.ReplyChan <- StableClkUpdatesPair{stableClock: stableClk, upds: txns}

}

func (logger *MemLogger) handleNextClkOldLogRequest(request LogOldNextClkArgs) {
	//Compare request clk with the previous clk that was sent
	clkCompare := request.PreviousClk.ComparePos(ReplicaID, *logger.log[logger.currLogPos].clk)

	if clkCompare == clocksi.EqualTs && logger.currLogPos < len(logger.log) {
		logger.currLogPos++
		request.ReplyChan <- BoolTimestampPair{bool: true, Timestamp: *logger.log[logger.currLogPos].clk}
	} else if clkCompare < clocksi.LowerTs {
		request.ReplyChan <- BoolTimestampPair{bool: true, Timestamp: *logger.log[logger.currLogPos].clk}
	} else {
		//TODO: Request clock to materializer
		request.ReplyChan <- BoolTimestampPair{bool: false, Timestamp: clocksi.DummyTs}
	}
}

func (logger *MemLogger) handleTxnOldLogRequest(request LogOldTxnArgs) {
	request.ReplyChan <- logger.log[logger.currLogPos].upds
}
