package antidote

import (
	fmt "fmt"
	"potionDB/src/clocksi"
	"potionDB/src/shared"
)

/*****Logging interface*****/
//TODO: Improve with a switch of channels (or select?). Better than use two routines with locks I think.

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
	ReadClk  clocksi.Timestamp
	CommitTs int64
	Upds     []*UpdateObjectParams //Should be downstream arguments
}

type LogClockArgs struct {
	SafeTs int64
}

type LogTxnArgs struct {
	LastTs    int64 //Note: This is actually unused as the logger keeps track of what was already sent to Replicator
	ReplyChan chan StableTsUpdatesPair
}

type StableTsUpdatesPair struct {
	upds     []PairClockUpdates
	stableTs int64
}

/*
type StableClkUpdatesPair struct {
	upds        []PairClockUpdates
	stableClock clocksi.Timestamp
}
*/

type LogRequestType byte

type BoolTimestampPair struct {
	bool
	clocksi.Timestamp
}

const (
	//Types of requests
	CommitLogRequest LogRequestType = 0 //From Mat
	ClockLogRequest  LogRequestType = 1 //From Mat
	TxnLogRequest    LogRequestType = 2 //From Repl
)

func (args LogCommitArgs) GetRequestType() (requestType LogRequestType) {
	return CommitLogRequest
}

func (args LogClockArgs) GetRequestType() (requestType LogRequestType) {
	return ClockLogRequest
}

func (args LogTxnArgs) GetRequestType() (requestType LogRequestType) {
	return TxnLogRequest
}

/*****In-Memory Logger implementation*****/

type MemLogger struct {
	started          bool
	log              []PairClockUpdates
	currLogPos       int
	matChan          chan LoggerRequest
	replChan         chan LoggerRequest
	partId           uint64
	mat              *Materializer            //Used to send the safeClk request
	replyReplChan    chan StableTsUpdatesPair //This is updated everytime we receive a request from the replicator
	logCapacityToUse int                      //Adjustable, depending on how big the logs tend to get when we replicate.
}

type PairClockUpdates struct {
	readClk  clocksi.Timestamp
	commitTs int64
	upds     []*UpdateObjectParams
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
	case ClockLogRequest:
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
		logger.matChan = make(chan LoggerRequest, 10)
		logger.replChan = make(chan LoggerRequest, 1)
		logger.started = true
		logger.partId = partId
		logger.mat = mat
		logger.logCapacityToUse = initLogCapacity
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
			switch request.GetRequestType() {
			case CommitLogRequest:
				logger.handleCommitLogRequest(request.LogRequestArgs.(LogCommitArgs))
			case ClockLogRequest:
				logger.handleClockLogRequest(request.LogRequestArgs.(LogClockArgs))
			}
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
	logger.log = append(logger.log, PairClockUpdates{readClk: request.ReadClk, commitTs: request.CommitTs, upds: request.Upds})
}

func (logger *MemLogger) handleClockLogRequest(request LogClockArgs) {
	//Now we can reply to the replicator's request. Now we know the latest stable clock in the partition and all the transactions until it.
	//Note: If shared.IsLogDisabled is true, this method will never execute, as there'll never be a clock log request.
	var txns []PairClockUpdates
	if logger.currLogPos == len(logger.log) {
		txns = nil
	} else {
		txns = logger.log[logger.currLogPos:]
		logger.currLogPos = len(logger.log)
		if !keepWholeLog {
			logger.calculateNextLogSize() //Updates logger.logCapacityToUse
			logger.log = make([]PairClockUpdates, 0, logger.logCapacityToUse)
			logger.currLogPos = 0
		}
	}
	logger.replyReplChan <- StableTsUpdatesPair{stableTs: request.SafeTs, upds: txns}
}

func (logger *MemLogger) handleTxnLogRequest(request LogTxnArgs) {
	if shared.IsLogDisabled {
		request.ReplyChan <- StableTsUpdatesPair{stableTs: clocksi.GetSystemCurrTime(), upds: []PairClockUpdates{}}
		return
	}
	logger.replyReplChan = request.ReplyChan
	//Request the latest stable clock from this partition's materializer. Wait for this reply in logger.matChan
	logger.mat.SendRequestToChannel(MaterializerRequest{MatRequestArgs: MatSafeClkArgs{}}, uint64(logger.partId))
}

//last: 5, toUse: 1000
func (logger *MemLogger) calculateNextLogSize() {
	if logger.logCapacityToUse <= initLogCapacity/4 {
		return //Don't change if it's already quite smaller than what we started with
	}
	factor := 1.0
	lastLogLen, floatCapacityToUse := float64(len(logger.log)), float64(logger.logCapacityToUse)
	//Optimization: if the logger had a lot more updates than logCapacityToUse, we'll increase the size of logCapacityToUse.
	//If on the other hand it had a lot less, we decrease it a bit (until a certain factor of the initial size)
	if lastLogLen > 100*floatCapacityToUse {
		//Quite big. We'll be aggressive on the increase.
		factor = (lastLogLen / floatCapacityToUse) / 10.0
	} else if lastLogLen > 10*floatCapacityToUse {
		//Let's be cautious and increase slowly.
		//The idea: it will increase between 2x and 6x, depending on how big logger.logCapacityToUse is.
		factor = (lastLogLen/floatCapacityToUse)/20.0 + 1
	} else if lastLogLen < floatCapacityToUse/10 {
		factor = (lastLogLen/floatCapacityToUse)*10.0 + 0.1
	} else if lastLogLen < floatCapacityToUse/100 {
		//Reduce heavily
		factor = (lastLogLen / floatCapacityToUse) * 10.0
	}
	logger.logCapacityToUse = int(floatCapacityToUse * factor)
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
*/
