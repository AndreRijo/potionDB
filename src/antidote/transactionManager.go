package antidote

//TODO: Try refactoring this to be inside another folder in antidote
//(e.g: antidote/transaction), and check if that still has circular import issues

import (
	"clocksi"
	"crdt"
	fmt "fmt"
	"math/rand"
	"sync"
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

type ReadObjectParams struct {
	KeyParams
	ReadArgs crdt.ReadArguments
}

type TransactionManagerRequest struct {
	TransactionId //TODO: Remove this, as most requests don't need it (iirc, only staticWrite, commit and abort use it)
	Timestamp     clocksi.Timestamp
	Args          TMRequestArgs
}

type TMRequestArgs interface {
	getRequestType() (requestType TMRequestType)
}

type TMReadArgs struct {
	ReadParams []ReadObjectParams
	ReplyChan  chan []crdt.State
}

type TMUpdateArgs struct {
	UpdateParams []UpdateObjectParams
	ReplyChan    chan TMUpdateReply
}

type TMStaticUpdateArgs struct {
	UpdateParams []UpdateObjectParams
	ReplyChan    chan TMStaticUpdateReply
}

type TMStaticReadArgs struct {
	ReadParams []ReadObjectParams
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

//Used by the Replicatoin Layer. Use a different thread to handle this
type TMRemoteTxn struct {
	ReplicaID int64
	Upds      []RemoteTxns
}

type TMStaticReadReply struct {
	States    []crdt.State
	Timestamp clocksi.Timestamp
}

type TMStaticUpdateReply struct {
	TransactionId
	Timestamp clocksi.Timestamp
	Err       error
}

type TMUpdateReply struct {
	Success bool
	Err     error
}

type TMStartTxnReply struct {
	TransactionId
	Timestamp clocksi.Timestamp
}

type TMCommitReply struct {
	Timestamp clocksi.Timestamp
	Err       error
}

type TMRequestType int

type ClientId uint64

/*
type TransactionId struct {
	ClientId  ClientId
	Timestamp clocksi.Timestamp
}
*/
type TransactionId uint64

type partSet map[uint64]struct{}

//"Extends" sync.Map with an additional load method
type SyncMap struct {
	sync.Map
}

type ProtectedClock struct {
	clocksi.Timestamp
	sync.Mutex
}

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
	//TODO: This should be configurable or received somehow
	ReplicaID int64 = 0
)

var (
	//TODO: Optimization: have each thread store his entry of txnPartitions, similary to what is done in the Materializer. This avoids having to use a map with locks (sync.Map)
	//map of txnID -> partitions which had at least one update of that txn
	//the normal go map isn't safe for concurrent writes even in different keys
	//txnPartitions map[TransactionId]partSet = make(map[TransactionId]partSet)
	txnPartitions SyncMap          = SyncMap{}
	remoteTxnChan chan TMRemoteTxn = make(chan TMRemoteTxn)
	//TODO: Currently downstreaming txns holds the lock until the whole process is done (i.e., all transactions are either downstreamed to Materialized or queued). This can be a problem if we need to wait for Materializer - use bounded chans?
	localClock ProtectedClock = ProtectedClock{
		Timestamp: clocksi.NewClockSiTimestamp(),
		Mutex:     sync.Mutex{},
	}
	downstreamQueue = make(map[int64][]RemoteTxns)
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

func makePartSet() (set partSet) {
	set = partSet(make(map[uint64]struct{}))
	return
}

func (set partSet) add(partId uint64) {
	set[partId] = struct{}{}
}

func (m *SyncMap) LoadSimple(key interface{}) (value interface{}) {
	value, _ = m.Load(key)
	return value
}

/////*****************TRANSACTION MANAGER CODE***********************/////

func Initialize() {
	loggers := InitializeMaterializer()
	(&Replicator{}).Initialize(loggers) //For now, we don't need to store the replicator instance, as it is independent and works by itself
	go handleRemoteTxnRequest()
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

func SendRemoteTxnRequest(request TMRemoteTxn) {
	remoteTxnChan <- request
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
	//remoteTxnRequest is handled separatelly

	return
}

func handleRemoteTxnRequest() {
	for {
		request := <-remoteTxnChan
		applyRemoteTxn(request.ReplicaID, request.Upds)
	}
}

//TODO: Group reads. Also, send a "read operation" instead of just key params.
func handleStaticTMRead(request TransactionManagerRequest) {
	readArgs := request.Args.(TMStaticReadArgs)
	//tsToUse := request.Timestamp
	localClock.Lock()
	tsToUse := localClock.Timestamp //TODO: Should we use here directly localClock or localClock.NextTimestamp()?
	localClock.Unlock()

	var currReadChan chan crdt.State = nil
	var currRequest MaterializerRequest
	states := make([]crdt.State, len(readArgs.ReadParams))

	//Now, ask to read the client requested version.
	for i, currRead := range readArgs.ReadParams {
		currReadChan = make(chan crdt.State)

		currRequest = MaterializerRequest{
			MatRequestArgs: MatStaticReadArgs{MatReadCommonArgs: MatReadCommonArgs{
				Timestamp:        tsToUse,
				ReadObjectParams: currRead,
				ReplyChan:        currReadChan,
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

	newTxnId := TransactionId(rand.Uint64())
	//1st step: discover involved partitions and group updates
	updsPerPartition := groupWrites(updateArgs.UpdateParams)

	replyChannels := make([]chan TimestampErrorPair, 0, len(updsPerPartition))
	var currChan chan TimestampErrorPair
	var currRequest MaterializerRequest
	//2nd step: send update operations to each involved partition
	for partId, partUpdates := range updsPerPartition {
		if partUpdates != nil {
			currChan = make(chan TimestampErrorPair)
			currRequest = MaterializerRequest{
				MatRequestArgs: MatStaticUpdateArgs{
					TransactionId: newTxnId,
					Updates:       partUpdates,
					ReplyChan:     currChan,
				},
			}
			replyChannels = append(replyChannels, currChan)
			SendRequestToChannel(currRequest, uint64(partId))
		}
	}

	var maxTimestamp *clocksi.Timestamp = &clocksi.DummyTs
	//Also 2nd step: wait for reply of each partition
	//TODO: Possibly paralelize? What if errors occour?
	for _, channel := range replyChannels {
		reply := <-channel
		if reply.Timestamp == nil {
			updateArgs.ReplyChan <- TMStaticUpdateReply{
				Timestamp: nil,
				Err:       reply.error,
			}
			return
		}
		if reply.Timestamp.IsHigherOrEqual(*maxTimestamp) {
			maxTimestamp = &reply.Timestamp
		}
	}

	//3rd step: send commit to involved partitions
	//TODO: Should I not assume that the 2nd phase of commit is fail-safe?
	commitReq := MaterializerRequest{MatRequestArgs: MatCommitArgs{
		TransactionId:   newTxnId,
		CommitTimestamp: *maxTimestamp,
	}}
	for partId, partUpdates := range updsPerPartition {
		if partUpdates != nil {
			SendRequestToChannel(commitReq, uint64(partId))
		}
	}
	localClock.Lock()
	localClock.Timestamp = localClock.Merge(*maxTimestamp)
	localClock.Unlock()

	//4th step: send ok to client
	updateArgs.ReplyChan <- TMStaticUpdateReply{
		TransactionId: newTxnId,
		Timestamp:     *maxTimestamp,
		Err:           nil,
	}

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

//TODO: Group reads.
func handleTMRead(request TransactionManagerRequest) {
	readArgs := request.Args.(TMReadArgs)
	tsToUse := request.Timestamp

	var currReadChan chan crdt.State = nil
	var currRequest MaterializerRequest
	states := make([]crdt.State, len(readArgs.ReadParams))

	//Now, ask to read the client requested version.
	for i, currRead := range readArgs.ReadParams {
		currReadChan = make(chan crdt.State)

		currRequest = MaterializerRequest{
			MatRequestArgs: MatReadArgs{MatReadCommonArgs: MatReadCommonArgs{
				Timestamp:        tsToUse,
				ReadObjectParams: currRead,
				ReplyChan:        currReadChan,
			}, TransactionId: request.TransactionId},
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
	var currRequest MaterializerRequest
	var partId uint64

	for id, partUpdates := range updsPerPartition {
		if partUpdates != nil {
			partId = uint64(id)
			currChan = make(chan BoolErrorPair)
			currRequest = MaterializerRequest{
				MatRequestArgs: MatUpdateArgs{
					TransactionId: request.TransactionId,
					Updates:       partUpdates,
					ReplyChan:     currChan,
				},
			}
			replyChannels = append(replyChannels, currChan)
			//Mark this partition as one of the involved in this txnId
			//if _, hasPart := txnPartitions[request.TransactionId][partId]; !hasPart {
			parts := txnPartitions.LoadSimple(request.TransactionId).(partSet)
			if _, hasPart := parts[partId]; !hasPart {
				parts.add(partId)
			}
			SendRequestToChannel(currRequest, partId)
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
		updateArgs.ReplyChan <- TMUpdateReply{
			Success: true,
			Err:     nil,
		}
	} else {
		//TODO: Send abort on error?
		updateArgs.ReplyChan <- TMUpdateReply{
			Success: false,
			Err:     fmt.Errorf(errString),
		}
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
	localClock.Lock()
	newClock := localClock.NextTimestamp()
	localClock.Unlock()
	var newTxnId TransactionId = TransactionId(rand.Uint64())
	txnPartitions.Store(newTxnId, makePartSet())

	startTxnArgs.ReplyChan <- TMStartTxnReply{TransactionId: newTxnId, Timestamp: newClock}
}

func handleTMCommit(request TransactionManagerRequest) {
	commitArgs := request.Args.(TMCommitArgs)

	//PREPARE
	involvedPartitions := txnPartitions.LoadSimple(request.TransactionId).(partSet)
	replyChannels := make([]chan clocksi.Timestamp, 0, len(involvedPartitions))
	var currRequest MaterializerRequest
	//TODO: Use bounded channels and send the same channel to every partition?
	var currChan chan clocksi.Timestamp

	//Send prepare to each partition involved
	for partId, _ := range involvedPartitions {
		currChan = make(chan clocksi.Timestamp)
		currRequest = MaterializerRequest{MatRequestArgs: MatPrepareArgs{TransactionId: request.TransactionId, ReplyChan: currChan}}
		replyChannels = append(replyChannels, currChan)
		SendRequestToChannel(currRequest, partId)
	}

	//Collect proposed timestamps and accept the maximum one
	var maxTimestamp *clocksi.Timestamp = &clocksi.DummyTs
	//TODO: Possibly paralelize?
	for _, channel := range replyChannels {
		replyTs := <-channel
		if replyTs.IsHigherOrEqual(*maxTimestamp) {
			maxTimestamp = &replyTs
		}
	}

	//COMMIT
	//Send commit to involved partitions
	//TODO: Should I not assume that the 2nd phase of commit is fail-safe?

	for partId, _ := range involvedPartitions {
		currRequest = MaterializerRequest{MatRequestArgs: MatCommitArgs{
			TransactionId:   request.TransactionId,
			CommitTimestamp: *maxTimestamp,
		}}
		SendRequestToChannel(currRequest, uint64(partId))
	}

	txnPartitions.Delete(request.TransactionId)

	localClock.Lock()
	localClock.Timestamp = localClock.Merge(*maxTimestamp)
	localClock.Unlock()

	//Send ok to client
	commitArgs.ReplyChan <- TMCommitReply{
		Timestamp: *maxTimestamp,
		Err:       nil,
	}
}

func handleTMAbort(request TransactionManagerRequest) {
	abortReq := MaterializerRequest{MatRequestArgs: MatAbortArgs{TransactionId: request.TransactionId}}
	for partId, _ := range txnPartitions.LoadSimple(request.TransactionId).(partSet) {
		SendRequestToChannel(abortReq, uint64(partId))
	}
	txnPartitions.Delete(request.TransactionId)
}

func applyRemoteTxn(replicaID int64, upds []RemoteTxns) {
	localClock.Lock()
	for i, txn := range upds {
		isLowerOrEqual := txn.Timestamp.IsLowerOrEqualExceptFor(localClock.Timestamp, replicaID)
		if isLowerOrEqual {
			downstreamRemoteTxn(replicaID, txn.Timestamp, txn.upds)
		} else {
			//Check if txn.upds is empty - if it is, then it's just a clock update, which can be applied right away
			if len(*txn.upds) == 0 {
				localClock.Timestamp = localClock.Timestamp.UpdatePos(replicaID, txn.Timestamp.GetPos(replicaID))
			} else {
				//Queue all others
				downstreamQueue[replicaID] = append(downstreamQueue[replicaID], upds[i:]...)
			}
			break
		}
	}
	checkPendingRemoteTxns()
	localClock.Unlock()
}

//Pre-condition: localClock's mutex is hold
func downstreamRemoteTxn(replicaID int64, txnClk clocksi.Timestamp, txnOps *map[int][]UpdateObjectParams) {
	var currRequest MaterializerRequest
	for partId, partOps := range *txnOps {
		currRequest = MaterializerRequest{MatRequestArgs: MatRemoteTxnArgs{
			ReplicaID: replicaID,
			Timestamp: txnClk,
			Upds:      partOps,
		}}
		SendRequestToChannel(currRequest, uint64(partId))
	}
	localClock.Timestamp = localClock.Timestamp.UpdatePos(replicaID, txnClk.GetPos(replicaID))
}

//Pre-condition: localClock's mutex is hold
func checkPendingRemoteTxns() {
	appliedAtLeastOne := true
	for appliedAtLeastOne {
		for replicaID, pendingTxns := range downstreamQueue {
			if !appliedAtLeastOne {
				break
			}
			appliedAtLeastOne = false
			for _, txn := range pendingTxns {
				isLowerOrEqual := txn.Timestamp.IsLowerOrEqualExceptFor(localClock.Timestamp, replicaID)
				if isLowerOrEqual {
					downstreamRemoteTxn(replicaID, txn.Timestamp, txn.upds)
					appliedAtLeastOne = true
					if len(pendingTxns) > 1 {
						pendingTxns = pendingTxns[1:]
					} else {
						pendingTxns = nil
					}
				} else {
					break
				}
			}
			if pendingTxns == nil {
				delete(downstreamQueue, replicaID)
			} else {
				downstreamQueue[replicaID] = pendingTxns
			}
		}
	}
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
