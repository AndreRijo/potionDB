package antidote

//TODO: Try refactoring this to be inside another folder in antidote
//(e.g: antidote/transaction), and check if that still has circular import issues

import (
	"clocksi"
	"crdt"
	fmt "fmt"
)

/////*****************TYPE DEFINITIONS***********************/////

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
	ReplyChan  chan TMReadReply
}

type TMUpdateArgs struct {
	UpdateParams []UpdateObjectParams
	ReplyChan    chan TMUpdateReply
}

type TMConnLostArgs struct {
}

type TMReadReply struct {
	States    []crdt.State
	Timestamp clocksi.Timestamp
}

type TMUpdateReply struct {
	Timestamp clocksi.Timestamp
	Err       error
}

type TMRequestType int

type ClientId uint64

type TransactionId struct {
	ClientId  ClientId
	Timestamp clocksi.Timestamp
}

/////*****************CONSTANTS AND VARIABLES***********************/////

const (
	readTMRequest   TMRequestType = 0
	writeTMRequest  TMRequestType = 1
	lostConnRequest TMRequestType = 255
)

/*
var (
	currTimestamp clocksi.Timestamp = clocksi.ClockSiTimestamp{}.NewTimestamp()
)
*/

/////*****************TYPE METHODS***********************/////

func (args TMReadArgs) getRequestType() (requestType TMRequestType) {
	requestType = readTMRequest
	return
}

func (args TMUpdateArgs) getRequestType() (requestType TMRequestType) {
	requestType = writeTMRequest
	return
}

func (args TMConnLostArgs) getRequestType() (requestType TMRequestType) {
	requestType = lostConnRequest
	return
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
		//fmt.Println("TransactionManager goroutine", id, "received a request!")
		stop = handleTMRequest(request)
	}

	fmt.Println("TransactionManager - connection lost, shutting down goroutine.")
}

func handleTMRequest(request TransactionManagerRequest) (shouldStop bool) {
	shouldStop = false

	switch request.Args.getRequestType() {
	case readTMRequest:
		handleTMRead(request)
	case writeTMRequest:
		handleTMWrite(request)
	case lostConnRequest:
		shouldStop = true
	}

	return
}

//TODO: Maybe I didn't had to find a common timestamp and I should had just asked for the client's version...?
//Obviously in that case it would still block if a partition doesn't yet have a high enough version.
func handleTMRead(request TransactionManagerRequest) {
	readArgs := request.Args.(TMReadArgs)

	smallestTS := findCommonTimestamp(readArgs.ObjsParams, request.Timestamp)
	if smallestTS.IsLower(request.Timestamp) {
		smallestTS = request.Timestamp
	}

	var currReadChan chan crdt.State = nil
	var currRequest MaterializerRequest
	states := make([]crdt.State, len(readArgs.ObjsParams))

	//Now, ask to read the version corresponding to timestamp smallestTS.
	for i, currRead := range readArgs.ObjsParams {
		currReadChan = make(chan crdt.State)

		currRequest = MaterializerRequest{
			MatRequestArgs: MatReadArgs{
				Timestamp: smallestTS,
				KeyParams: currRead,
				ReplyChan: currReadChan,
			},
		}
		SendRequest(currRequest)
		states[i] = <-currReadChan
		close(currReadChan)
	}

	readArgs.ReplyChan <- TMReadReply{
		States:    states,
		Timestamp: smallestTS,
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

	return smallestTS
}

//TODO: Separate in parts?
//Note: For now this corresponds to static writes.
func handleTMWrite(request TransactionManagerRequest) {
	updateArgs := request.Args.(TMUpdateArgs)
	updatesPerPartition := make([]*MatStaticUpdateArgs, nGoRoutines)
	envolvedPartitions := make([]uint64, 0, nGoRoutines)

	var currChanKey uint64
	//1st step: discover envolved partitions and group updates
	for _, upd := range updateArgs.UpdateParams {
		currChanKey = GetChannelKey(upd.KeyParams)
		if updatesPerPartition[currChanKey] == nil {
			updatesPerPartition[currChanKey] = &MatStaticUpdateArgs{
				//Slice with initial length of 0 (empty) and a default capacity. Resize is automatic if when using append
				Updates:       make([]UpdateObjectParams, 0, len(updateArgs.UpdateParams)*2/int(nGoRoutines)),
				TransactionId: request.TransactionId,
				ReplyChan:     make(chan TimestampErrorPair),
			}
			envolvedPartitions = append(envolvedPartitions, currChanKey)
		}
		updatesPerPartition[currChanKey].Updates = append(updatesPerPartition[currChanKey].Updates, upd)
	}

	//2nd step: send update operations to each envolved partition
	for _, partId := range envolvedPartitions {
		matRequest := MaterializerRequest{MatRequestArgs: updatesPerPartition[partId]}
		SendRequestToChannel(matRequest, partId)
	}

	var maxTimestamp *clocksi.Timestamp
	//Also 2nd step: wait for reply of each partition
	//TODO: Possibly paralelize? What if errors occour?
	for _, partId := range envolvedPartitions {
		reply := <-updatesPerPartition[partId].ReplyChan
		if reply.Timestamp == nil {
			updateArgs.ReplyChan <- TMUpdateReply{
				Timestamp: nil,
				Err:       reply.error,
			}
			return
		}
		if reply.Timestamp.IsHigherOrEqual(*maxTimestamp) {
			maxTimestamp = &reply.Timestamp
		}
	}

	//3rd step: send commit to envolved partitions
	//TODO: Should I not assume that the 2nd phase of commit is fail-safe?
	for _, partId := range envolvedPartitions {
		SendRequest(MaterializerRequest{MatRequestArgs: MatCommitArgs{
			TransactionId:   request.TransactionId,
			CommitTimestamp: *maxTimestamp,
			ChannelId:       partId,
		}})
	}

	//4th step: send ok to client
	updateArgs.ReplyChan <- TMUpdateReply{
		Timestamp: *maxTimestamp,
		Err:       nil,
	}

	/*
		Algorithm:
			1st step: discover envolved partitions and group writes
				- for update in writeRequest.UpdateParams
					- getPartitionKey
					- add update to list
			2nd step: send update operations to each envolved partition and collect proposed timestamp
				- for each partition envolved
					- send list of updates
					- wait for proposed timestamp
					- if proposed timestamp > highest proposed timestamp so far
						highest timestamp = proposed timestamp
			3rd step: send commit to envolved partitions
				- for each partition envolved
					- commit(highest timestamp)
			4th step: send ok to client
	*/
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
