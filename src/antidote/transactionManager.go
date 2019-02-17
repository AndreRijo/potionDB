package antidote

//TODO: Try refactoring this to be inside another folder in antidote
//(e.g: antidote/transaction), and check if that still has circular import issues

import (
	"clocksi"
	"crdt"
	fmt "fmt"
)

/////*****************TYPE DEFINITIONS***********************/////
//TODO: Extract requests types, replies and methods to another file

type KeyParams struct {
	Key      string
	CrdtType CRDTType
	Bucket   string
}

type UpdateObjectParams struct {
	KeyParams
	UpdateArgs crdt.UpdateArguments
}

//TODO: Remove this one, as it is only used by the client
type ReadObjectParams struct {
	KeyParams //This grants access to any of the fields in KeyParams
}

type TransactionManagerRequest struct {
	TransactionId
	Args TMRequestArgs
}

type TMRequestArgs interface {
	getRequestType() (requestType TMRequestType)
}

type TMReadArgs struct {
	ObjsParams []KeyParams
	ReplyChan  chan []crdt.State
}

type TMUpdateArgs struct {
	UpdateParams []UpdateObjectParams
	ReplyChan    chan TMUpdateReply
}

type TMStaticUpdateArgs struct {
	TMUpdateArgs
}

type TMStaticReadArgs struct {
	ObjsParams []KeyParams
	ReplyChan  chan TMStaticReadReply
}

type TMConnLostArgs struct {
}

type TMStartTxnArgs struct {
	ReplyChan chan TMStartTxnReply
}

type TMCommitArgs struct {
	ReplyChan chan TMCommitReply
}

type TMAbortArgs struct {
}

type TMStaticReadReply struct {
	States    []crdt.State
	Timestamp clocksi.Timestamp
}

type TMUpdateReply struct {
	TMTimestampErrorPairReply
}

//TODO: I need to refactor this error thing most prob...
type TMTimestampErrorPairReply struct {
	Timestamp clocksi.Timestamp
	Err       error
}

type TMStartTxnReply struct {
	Timestamp clocksi.Timestamp
}

type TMCommitReply struct {
	TMTimestampErrorPairReply
}

type TMRequestType int

type ClientId uint64

type TransactionId struct {
	ClientId  ClientId
	Timestamp clocksi.Timestamp
}

type matReqSet map[chan MaterializerRequest]struct{}

/////*****************CONSTANTS AND VARIABLES***********************/////

const (
	readStaticTMRequest   TMRequestType = 0
	updateStaticTMRequest TMRequestType = 1
	readTMRequest         TMRequestType = 2
	updateTMRequest       TMRequestType = 3
	startTxnTMRequest     TMRequestType = 4
	commitTMRequest       TMRequestType = 5
	abortTMRequest        TMRequestType = 6
	lostConnRequest       TMRequestType = 255
)

var (
	txnPartitions map[TransactionId]matReqSet //map of txnID -> partitions which had at least one update of that txn
)

/////*****************TYPE METHODS***********************/////

func (args TMStaticReadArgs) getRequestType() (requestType TMRequestType) {
	return readStaticTMRequest
}

func (args TMStaticUpdateArgs) getRequestType() (requestType TMRequestType) {
	return updateStaticTMRequest
}

func (args TMReadArgs) getRequestType() (requestType TMRequestType) {
	return readTMRequest
}

func (args TMUpdateArgs) getRequestType() (requestType TMRequestType) {
	return updateTMRequest
}

func (args TMConnLostArgs) getRequestType() (requestType TMRequestType) {
	return lostConnRequest
}

func (args TMStartTxnArgs) getRequestType() (requestType TMRequestType) {
	return startTxnTMRequest
}

func (args TMCommitArgs) getRequestType() (requestType TMRequestType) {
	return commitTMRequest
}

func (args TMAbortArgs) getRequestType() (requestType TMRequestType) {
	return abortTMRequest
}

func makeMatReqSet() (set matReqSet) {
	set = matReqSet(make(map[chan MaterializerRequest]struct{}))
	return
}

func (set matReqSet) add(channel chan MaterializerRequest) {
	set[channel] = struct{}{}
}

/////*****************TRANSACTION MANAGER CODE***********************/////

func Initialize() {
	InitializeMaterializer()
}

func CreateKeyParams(key string, crdtType CRDTType, bucket string) (keyParams KeyParams) {
	keyParams = KeyParams{
		Key:      key,
		CrdtType: crdtType,
		Bucket:   bucket,
	}
	return
}

//Starts a goroutine to handle the client requests. Returns a channel to communicate with that goroutine
func CreateClientHandler() (channel chan TransactionManagerRequest) {
	channel = make(chan TransactionManagerRequest)
	go listenForProtobufRequests(channel)
	return
}

func listenForProtobufRequests(channel chan TransactionManagerRequest) {
	stop := false
	for !stop {
		request := <-channel
		stop = handleTMRequest(request)
	}

	fmt.Println("TransactionManager - connection lost, shutting down goroutine.")
}

func handleTMRequest(request TransactionManagerRequest) (shouldStop bool) {
	shouldStop = false

	switch request.Args.getRequestType() {
	case readStaticTMRequest:
		handleStaticTMRead(request)
	case updateStaticTMRequest:
		handleStaticTMUpdate(request)
	case readTMRequest:
		handleTMRead(request)
	case updateTMRequest:
		handleTMUpdate(request)
	case startTxnTMRequest:
		handleTMStartTxn(request)
	case commitTMRequest:
		handleTMCommit(request)
	case abortTMRequest:
		handleTMAbort(request)
	case lostConnRequest:
		shouldStop = true
	}

	return
}

//TODO: Group reads. Also, send a "read operation" instead of just key params.
func handleStaticTMRead(request TransactionManagerRequest) {
	readArgs := request.Args.(TMStaticReadArgs)
	tsToUse := request.Timestamp

	var currReadChan chan crdt.State = nil
	var currRequest MaterializerRequest
	states := make([]crdt.State, len(readArgs.ObjsParams))

	//Now, ask to read the client requested version.
	for i, currRead := range readArgs.ObjsParams {
		currReadChan = make(chan crdt.State)

		currRequest = MaterializerRequest{
			MatRequestArgs: MatStaticReadArgs{MatReadArgs: MatReadArgs{
				Timestamp: tsToUse,
				KeyParams: currRead,
				ReplyChan: currReadChan,
			}},
		}
		SendRequest(currRequest)
		//TODO: Wait for reply in different for
		states[i] = <-currReadChan
		close(currReadChan)
	}

	readArgs.ReplyChan <- TMStaticReadReply{
		States:    states,
		Timestamp: tsToUse,
	}

	/*
		Algorithm:
			First step: discover what timestamp to use
				for each read
					find channel;
					if it is a new channel
						ask for TS
						if TS < previousTS
							previousTS = TS
							if previousTS < clientTS
								panic for now (in future: put read on hold)
					if allChannelsConsulted
						break
			Second step: create a read channel for each channel to use (on implementation a new channel is created for each read)
				for each channel
					create channel
			Third step: perform read on smallest timestamp OR put on hold
				for each read
					send request
					wait for state
					//TODO: Paralelize the requests? This includes the TS ones
			Four step: return states
	*/

}

//TODO: Separate in parts?
func handleStaticTMUpdate(request TransactionManagerRequest) {
	updateArgs := request.Args.(TMStaticUpdateArgs)

	//1st step: discover involved partitions and group updates
	updsPerPartition := groupWrites(updateArgs.UpdateParams)

	replyChannels := make([]chan TimestampErrorPair, 0, len(updsPerPartition))
	var currChan chan TimestampErrorPair
	//2nd step: send update operations to each involved partition
	for partId, partUpdates := range updsPerPartition {
		if partUpdates != nil {
			currChan = make(chan TimestampErrorPair)
			matRequest := MaterializerRequest{
				MatRequestArgs: MatStaticUpdateArgs{
					TransactionId: request.TransactionId,
					Updates:       partUpdates,
					ReplyChan:     currChan,
				},
			}
			replyChannels = append(replyChannels, currChan)
			SendRequestToChannel(matRequest, uint64(partId))
		}
	}

	var maxTimestamp *clocksi.Timestamp = &clocksi.DummyTs
	//Also 2nd step: wait for reply of each partition
	//TODO: Possibly paralelize? What if errors occour?
	for _, channel := range replyChannels {
		reply := <-channel
		if reply.Timestamp == nil {
			updateArgs.ReplyChan <- TMUpdateReply{TMTimestampErrorPairReply{
				Timestamp: nil,
				Err:       reply.error,
			}}
			return
		}
		if reply.Timestamp.IsHigherOrEqual(*maxTimestamp) {
			maxTimestamp = &reply.Timestamp
		}
	}

	//3rd step: send commit to involved partitions
	//TODO: Should I not assume that the 2nd phase of commit is fail-safe?
	commitReq := MaterializerRequest{MatRequestArgs: MatCommitArgs{
		TransactionId:   request.TransactionId,
		CommitTimestamp: *maxTimestamp,
	}}
	for partId := range updsPerPartition {
		SendRequestToChannel(commitReq, uint64(partId))
	}

	//4th step: send ok to client
	updateArgs.ReplyChan <- TMUpdateReply{TMTimestampErrorPairReply{
		Timestamp: *maxTimestamp,
		Err:       nil,
	}}

	/*
		Algorithm:
			1st step: discover involved partitions and group writes
				- for update in writeRequest.UpdateParams
					- getPartitionKey
					- add update to list
			2nd step: send update operations to each involved partition and collect proposed timestamp
				- for each partition involved
					- send list of updates
					- wait for proposed timestamp
					- if proposed timestamp > highest proposed timestamp so far
						highest timestamp = proposed timestamp
			3rd step: send commit to involved partitions
				- for each partition
					- commit(highest timestamp)
			4th step: send ok to client
	*/
}

func askForCommitTs(txnId TransactionId) {
	for partition := range txnPartitions {

		//Send timestamp request; store answer channels in var
	}
	for reply := range replyChans {
		//Read timestamp; save the biggest.
	}
	//TODO: Update the current timestamp?
}

//TODO: Group reads.
func handleTMRead(request TransactionManagerRequest) {
	readArgs := request.Args.(TMReadArgs)
	tsToUse := request.Timestamp

	var currReadChan chan crdt.State = nil
	var currRequest MaterializerRequest
	states := make([]crdt.State, len(readArgs.ObjsParams))

	//Now, ask to read the client requested version.
	for i, currRead := range readArgs.ObjsParams {
		currReadChan = make(chan crdt.State)

		currRequest = MaterializerRequest{
			MatRequestArgs: MatReadArgs{
				Timestamp: tsToUse,
				KeyParams: currRead,
				ReplyChan: currReadChan,
			},
		}
		SendRequest(currRequest)
		//TODO: Wait for reply in different for
		states[i] = <-currReadChan
		close(currReadChan)
	}

	readArgs.ReplyChan <- states
}

func handleTMUpdate(request TransactionManagerRequest) {
	updateArgs := request.Args.(TMUpdateArgs)

	updsPerPartition := groupWrites(updateArgs.UpdateParams)

	replyChannels := make([]chan BoolErrorPair, 0, len(updsPerPartition))
	var currChan chan BoolErrorPair

	for partId, partUpdates := range updsPerPartition {
		if partUpdates != nil {
			currChan = make(chan BoolErrorPair)
			matRequest := MaterializerRequest{
				MatRequestArgs: MatUpdateArgs{
					TransactionId: request.TransactionId,
					Updates:       partUpdates,
					ReplyChan:     currChan,
				},
			}
			replyChannels = append(replyChannels, currChan)
			SendRequestToChannel(matRequest, uint64(partId))
		}
	}

	var errString = ""
	//TODO: Possibly paralelize? What if errors occour?
	for _, channel := range replyChannels {
		reply := <-channel
		if reply.error != nil {
			errString += reply.Error()
		}
	}

	if errString == "" {
		updateArgs.ReplyChan <- TMUpdateReply{TMBoolErrorPairReply{
			Success: true,
			Err:     nil,
		}}
	} else {
		updateArgs.ReplyChan <- TMUpdateReply{TMBoolErrorPairReply{
			Success: false,
			Err:     error(errString),
		}}
	}

}

/*
	Returns an array in which each index corresponds to one partition.
	Associated to each index is the list of reads that belong to the referred partition
*/
func groupReads(reads []KeyParams) (readsPerPartition [][]KeyParams) {
	readsPerPartition = make([][]KeyParams, nGoRoutines)
	var currChanKey uint64

	for _, read := range reads {
		currChanKey = GetChannelKey(read)
		if readsPerPartition[currChanKey] == nil {
			readsPerPartition[currChanKey] = make([]KeyParams, 0, len(reads)*2/int(nGoRoutines))
		}
		readsPerPartition[currChanKey] = append(readsPerPartition[currChanKey], read)
	}

	return
}

/*
	Returns an array in which each index corresponds to one partition.
	Associated to each index is the list of writes that belong to the referred partition
*/
func groupWrites(updates []UpdateObjectParams) (updsPerPartition [][]UpdateObjectParams) {
	updsPerPartition = make([][]UpdateObjectParams, nGoRoutines)
	var currChanKey uint64

	for _, upd := range updates {
		currChanKey = GetChannelKey(upd.KeyParams)
		if updsPerPartition[currChanKey] == nil {
			updsPerPartition[currChanKey] = make([]UpdateObjectParams, 0, len(updates)*2/int(nGoRoutines))
		}
		updsPerPartition[currChanKey] = append(updsPerPartition[currChanKey], upd)
	}

	return
}

func handleTMStartTxn(request TransactionManagerRequest) {
	startTxnArgs := request.Args.(TMStartTxnArgs)

	//TODO: Ensure that the new clock is higher than the one received from the client. Also, take in consideration txn properties?
	newClock := clocksi.NewClockSiTimestamp().NextTimestamp()
	newTxnId := TransactionId{
		ClientId:  request.ClientId,
		Timestamp: newClock,
	}
	txnPartitions[newTxnId] = makeMatReqSet()

	startTxnArgs.ReplyChan <- TMStartTxnReply{Timestamp: newClock}
}

func handleTMCommit(request TransactionManagerRequest) {
	commitArgs := request.Args.(TMCommitArgs)

	ignore(commitArgs)
	//TODO: At the end, clean up txnPartitions.
}

func handleTMAbort(request TransactionManagerRequest) {
	abortArgs := request.Args.(TMAbortArgs)

	ignore(abortArgs)
	//TODO: At the end, clean up txnPartitions.
}

//TODO: Possibly cache the hashing results and return them? That would allow to include them in the requests and paralelize the read requests
func findCommonTimestamp(objsParams []KeyParams, clientTs clocksi.Timestamp) (ts clocksi.Timestamp) {
	verifiedPartitions := make([]bool, nGoRoutines)
	var nVerified uint64 = 0
	readChannels := make([]chan crdt.State, nGoRoutines)
	var smallestTS clocksi.Timestamp = nil

	//Variables local to the first for. To avoid unecessary redeclarations
	var currChanKey uint64
	for _, currRead := range objsParams {
		currChanKey = GetChannelKey(currRead)
		//Still haven't verified this transaction
		if !verifiedPartitions[currChanKey] {

			nVerified++
			replyTSChan := make(chan clocksi.Timestamp)
			readChannels[currChanKey] = make(chan crdt.State)
			currTSRequest := MaterializerRequest{
				MatRequestArgs: MatVersionArgs{
					ReplyChan: replyTSChan,
					ChannelId: currChanKey,
				},
			}
			SendRequestToChannel(currTSRequest, currChanKey)
			currTS := <-replyTSChan
			close(replyTSChan)

			if smallestTS == nil || currTS.IsLowerOrEqual(smallestTS) {
				smallestTS = currTS
			}
			//Already verified all partitions, no need to continue
			if nVerified == nGoRoutines {
				break
			}
		}
	}
	if smallestTS.IsLower(clientTs) {
		smallestTS = clientTs
	}
	return smallestTS
}

//Temporary method. This is used to avoid compile errors on unused variables
//This unused variables mark stuff that isn't being processed yet.
func ignore(any interface{}) {

}

/***** OLD CODE FOR GOROUTINE WITHOUT TIMESTAMPS LOGIC VERSION *****/

/*
func HandleStaticReadObjects(objsParams []ReadObjectParams, lastClock clocksi.Timestamp) (states []crdt.State, ts clocksi.Timestamp) {
		ts := request.Timestamp.NextTimestamp()

		replyChan := make(chan []crdt.State)

		matRequest := MaterializerRequest{
			Args: MatReadArgs{
				ObjsParams: readRequest.ObjsParams,
				ReplyChan:  replyChan,
			},
			Timestamp: ts,
		}
		SendRequest(matRequest)
		states := <-replyChan
		close(replyChan)

		readRequest.ReplyChan <- TMReadReply{
			States:    states,
			Timestamp: ts,
		}
	}
*/

/*
func handleTMWrite(request TransactionManagerRequest) {
		//ts := request.Timestamp.NextTimestamp()
		writeRequest := request.Args.(TMUpdateArgs)
		replyChan := make(chan bool)

		for _, upd := range writeRequest.UpdateParams {
			request := MaterializerRequest{
				MatRequestArgs: MatUpdateArgs{
					UpdateObjectParams: upd,
					Timestamp:          request.Timestamp,
					ReplyChan:          replyChan,
				},
			}
			SendRequest(request)
			<-replyChan
		}
		close(replyChan)

		writeRequest.ReplyChan <- TMUpdateReply{
			Timestamp: request.Timestamp,
			Err:       nil,
		}
	}
*/

/***** OLD CODE FOR NO-GOROUTINE TM VERSION *****/

/*
type ReadObjectParams struct {
	KeyParams //This grants access to any of the fields in KeyParams
}

type UpdateObjectParams struct {
	KeyParams
	UpdateArgs crdt.UpdateArguments
}

//For now ignore the client's timestamp
//TODO: Actually take in consideration the client's timestamp
func HandleStaticReadObjects(objsParams []ReadObjectParams, lastClock clocksi.Timestamp) (states []crdt.State, ts clocksi.Timestamp) {
	ts = clocksi.NextTimestamp()
	states = make([]crdt.State, len(objsParams))
	//TODO: Couple this in a single request?
	replyChan := make(chan crdt.State)

	for i, obj := range objsParams {
		request := MaterializerRequest{
			KeyParams: obj.KeyParams,
			Args: MatReadArgs{
				ReplyChan: replyChan,
			},
			Timestamp: lastClock,
		}
		SendRequest(request)
		states[i] = <-replyChan
	}
	return
}

//For now ignore the client's timestamp
//TODO: Actually take in consideration the client's timestamp. Also, errors?
func HandleStaticUpdateObjects(updates []UpdateObjectParams, lastClock clocksi.Timestamp) (ts clocksi.Timestamp, err error) {
	ts = clocksi.NextTimestamp()
	//TODO: Couple this in a single request?
	replyChan := make(chan bool)
	for _, upd := range updates {
		request := MaterializerRequest{
			KeyParams: upd.KeyParams,
			Args: MatUpdateArgs{
				UpdateArguments: upd.UpdateArgs,
				ReplyChan:       replyChan,
			},
			Timestamp: lastClock,
		}
		SendRequest(request)
		<-replyChan
	}
	err = nil
	return
}

*/
