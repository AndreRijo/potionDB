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
	StableTs  int64
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

type ongoingTxn struct {
	TransactionId
	partSet
}

type partSet map[uint64]struct{}

type ProtectedClock struct {
	clocksi.Timestamp
	sync.Mutex
}

type TransactionManager struct {
	mat           *Materializer
	remoteTxnChan chan TMRemoteTxn
	//TODO: Currently downstreaming txns holds the lock until the whole process is done (i.e., all transactions are either downstreamed to Materialized or queued). This can be a problem if we need to wait for Materializer - use bounded chans?
	localClock         ProtectedClock
	downstreamQueue    map[int64][]RemoteTxns
	downstreamClkQueue map[int64]int64 //Stores clock updates of replicas that have txns in queue
	replicator         *Replicator
	replicaID          int64
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

func (txnPartitions *ongoingTxn) reset() {
	txnPartitions.TransactionId = 0
	txnPartitions.partSet = nil
}

/////*****************TRANSACTION MANAGER CODE***********************/////

func Initialize(replicaID int64) (tm *TransactionManager) {
	mat, loggers := InitializeMaterializer(replicaID)
	tm = &TransactionManager{
		mat:           mat,
		remoteTxnChan: make(chan TMRemoteTxn),
		localClock: ProtectedClock{
			Mutex:     sync.Mutex{},
			Timestamp: clocksi.NewClockSiTimestamp(),
		},
		downstreamQueue:    make(map[int64][]RemoteTxns),
		downstreamClkQueue: make(map[int64]int64),
		replicator:         &Replicator{},
		replicaID:          replicaID,
	}
	tm.replicator.Initialize(tm, loggers, replicaID)
	go tm.handleRemoteTxnRequest()
	return tm
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
func (tm *TransactionManager) CreateClientHandler() (channel chan TransactionManagerRequest) {
	channel = make(chan TransactionManagerRequest)
	go tm.listenForProtobufRequests(channel)
	return
}

func (tm *TransactionManager) SendRemoteTxnRequest(request TMRemoteTxn) {
	tm.remoteTxnChan <- request
}

func (tm *TransactionManager) listenForProtobufRequests(channel chan TransactionManagerRequest) {
	stop := false
	var txnPartitions *ongoingTxn = &ongoingTxn{}
	for !stop {
		request := <-channel
		stop = tm.handleTMRequest(request, txnPartitions)
	}

	fmt.Println("TransactionManager - connection lost, shutting down goroutine.")
}

func (tm *TransactionManager) handleTMRequest(request TransactionManagerRequest,
	txnPartitions *ongoingTxn) (shouldStop bool) {
	shouldStop = false

	switch request.Args.getRequestType() {
	case readStaticTMRequest:
		tm.handleStaticTMRead(request)
	case updateStaticTMRequest:
		tm.handleStaticTMUpdate(request)
	case readTMRequest:
		tm.handleTMRead(request)
	case updateTMRequest:
		tm.handleTMUpdate(request, txnPartitions)
	case startTxnTMRequest:
		tm.handleTMStartTxn(request, txnPartitions)
	case commitTMRequest:
		tm.handleTMCommit(request, txnPartitions)
	case abortTMRequest:
		tm.handleTMAbort(request, txnPartitions)
	case lostConnRequest:
		shouldStop = true
	}
	//remoteTxnRequest is handled separatelly

	return
}

func (tm *TransactionManager) handleRemoteTxnRequest() {
	for {
		request := <-tm.remoteTxnChan
		tm.applyRemoteTxn(request.ReplicaID, request.Upds, request.StableTs)
	}
}

//TODO: Group reads. Also, send a "read operation" instead of just key params.
func (tm *TransactionManager) handleStaticTMRead(request TransactionManagerRequest) {
	readArgs := request.Args.(TMStaticReadArgs)
	//tsToUse := request.Timestamp
	tm.localClock.Lock()
	tsToUse := tm.localClock.Timestamp //TODO: Should we use here directly localClock or localClock.NextTimestamp()?
	tm.localClock.Unlock()

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
		tm.mat.SendRequest(currRequest)
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
func (tm *TransactionManager) handleStaticTMUpdate(request TransactionManagerRequest) {
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
			tm.mat.SendRequestToChannel(currRequest, uint64(partId))
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
			tm.mat.SendRequestToChannel(commitReq, uint64(partId))
		}
	}
	tm.localClock.Lock()
	tm.localClock.Timestamp = tm.localClock.Merge(*maxTimestamp)
	tm.localClock.Unlock()

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
func (tm *TransactionManager) handleTMRead(request TransactionManagerRequest) {
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
		tm.mat.SendRequest(currRequest)
		//TODO: Wait for reply in different for
		states[i] = <-currReadChan
		close(currReadChan)
	}

	readArgs.ReplyChan <- states
}

func (tm *TransactionManager) handleTMUpdate(request TransactionManagerRequest, txnPartitions *ongoingTxn) {
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
			if _, hasPart := txnPartitions.partSet[partId]; !hasPart {
				txnPartitions.add(partId)
			}
			tm.mat.SendRequestToChannel(currRequest, partId)
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

func (tm *TransactionManager) handleTMStartTxn(request TransactionManagerRequest, txnPartitions *ongoingTxn) {
	startTxnArgs := request.Args.(TMStartTxnArgs)

	//TODO: Ensure that the new clock is higher than the one received from the client. Also, take in consideration txn properties?
	tm.localClock.Lock()
	newClock := tm.localClock.NextTimestamp(tm.replicaID)
	tm.localClock.Unlock()
	txnPartitions.TransactionId = TransactionId(rand.Uint64())
	txnPartitions.partSet = makePartSet()

	startTxnArgs.ReplyChan <- TMStartTxnReply{TransactionId: txnPartitions.TransactionId, Timestamp: newClock}
	return
}

func (tm *TransactionManager) handleTMCommit(request TransactionManagerRequest, txnPartitions *ongoingTxn) {
	commitArgs := request.Args.(TMCommitArgs)

	//PREPARE
	involvedPartitions := txnPartitions.partSet
	replyChannels := make([]chan clocksi.Timestamp, 0, len(involvedPartitions))
	var currRequest MaterializerRequest
	//TODO: Use bounded channels and send the same channel to every partition?
	var currChan chan clocksi.Timestamp

	//Send prepare to each partition involved
	for partId, _ := range involvedPartitions {
		currChan = make(chan clocksi.Timestamp)
		currRequest = MaterializerRequest{MatRequestArgs: MatPrepareArgs{TransactionId: request.TransactionId, ReplyChan: currChan}}
		replyChannels = append(replyChannels, currChan)
		tm.mat.SendRequestToChannel(currRequest, partId)
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
		tm.mat.SendRequestToChannel(currRequest, uint64(partId))
	}

	tm.localClock.Lock()
	tm.localClock.Timestamp = tm.localClock.Merge(*maxTimestamp)
	tm.localClock.Unlock()

	txnPartitions.reset()

	//Send ok to client
	commitArgs.ReplyChan <- TMCommitReply{
		Timestamp: *maxTimestamp,
		Err:       nil,
	}
}

func (tm *TransactionManager) handleTMAbort(request TransactionManagerRequest, txnPartitions *ongoingTxn) {
	abortReq := MaterializerRequest{MatRequestArgs: MatAbortArgs{TransactionId: request.TransactionId}}
	for partId, _ := range txnPartitions.partSet {
		tm.mat.SendRequestToChannel(abortReq, uint64(partId))
	}
	txnPartitions.reset()
}

func (tm *TransactionManager) applyRemoteTxn(replicaID int64, upds []RemoteTxns, stableTs int64) {
	fmt.Println("[TM", tm.replicaID, "]Started applying remote txn")
	tm.localClock.Lock()
	for i, txn := range upds {
		fmt.Println("[TM", tm.replicaID, "]Detected a remote txn. Local clk:", tm.localClock.Timestamp, ". Remote clk:", txn.Timestamp)
		isLowerOrEqual := txn.Timestamp.IsLowerOrEqualExceptFor(tm.localClock.Timestamp, tm.replicaID, replicaID)
		if isLowerOrEqual {
			tm.downstreamRemoteTxn(replicaID, txn.Timestamp, txn.upds)
		} else {
			//Queue all others
			fmt.Println("[TM", tm.replicaID, "]Queuing remote txn")
			tm.downstreamQueue[replicaID] = append(tm.downstreamQueue[replicaID], upds[i:]...)
			break
		}
	}
	//All txns were applied, so it's safe to update the local clock of both TM and materializer.
	if tm.downstreamQueue[replicaID] == nil {
		tm.localClock.Timestamp = tm.localClock.Timestamp.UpdatePos(replicaID, stableTs)
		tm.mat.SendRequestToAllChannels(MaterializerRequest{MatRequestArgs: MatClkPosUpdArgs{ReplicaID: replicaID, StableTs: stableTs}})
	} else {
		tm.downstreamClkQueue[replicaID] = stableTs
	}
	fmt.Println("[TM", tm.replicaID, "]Finished applying remote txn")
	tm.checkPendingRemoteTxns()
	tm.localClock.Unlock()
}

//Pre-condition: localClock's mutex is hold
func (tm *TransactionManager) downstreamRemoteTxn(replicaID int64, txnClk clocksi.Timestamp, txnOps *map[int][]UpdateObjectParams) {
	fmt.Println("[TM", tm.replicaID, "]Started downstream remote txn")
	var currRequest MaterializerRequest
	for partId, partOps := range *txnOps {
		currRequest = MaterializerRequest{MatRequestArgs: MatRemoteTxnArgs{
			ReplicaID: replicaID,
			Timestamp: txnClk,
			Upds:      partOps,
		}}
		fmt.Println("[TM", tm.replicaID, "]Sending remoteTxn request to Materializer")
		if len(partOps) > 0 {
			fmt.Println("[TM", tm.replicaID, "]Partition", partId, "has operations:", partOps)
		} else {
			fmt.Println("[TM", tm.replicaID, "]Partition", partId, "has NO operations:", partOps)
		}
		tm.mat.SendRequestToChannel(currRequest, uint64(partId))
	}
	tm.localClock.Timestamp = tm.localClock.Timestamp.UpdatePos(replicaID, txnClk.GetPos(replicaID))
	fmt.Println("[TM", tm.replicaID, "]Finished downstream remote txn")
}

//Pre-condition: localClock's mutex is hold
func (tm *TransactionManager) checkPendingRemoteTxns() {
	fmt.Println("[TM", tm.replicaID, "]Started checking for pending remote txns")
	appliedAtLeastOne := true
	//No txns pending, can return right away
	if len(tm.downstreamQueue) == 0 {
		fmt.Println("[TM", tm.replicaID, "]Finished checking for pending remote txns")
		return
	}
	for appliedAtLeastOne {
		for replicaID, pendingTxns := range tm.downstreamQueue {
			if !appliedAtLeastOne {
				break
			}
			appliedAtLeastOne = false
			for _, txn := range pendingTxns {
				isLowerOrEqual := txn.Timestamp.IsLowerOrEqualExceptFor(tm.localClock.Timestamp, tm.replicaID, replicaID)
				if isLowerOrEqual {
					tm.downstreamRemoteTxn(replicaID, txn.Timestamp, txn.upds)
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
				//Update clock with the previously received stable clock and delte the entries in the queues relative to replicaID.
				replicaClkValue := tm.downstreamClkQueue[replicaID]
				tm.localClock.Timestamp = tm.localClock.Timestamp.UpdatePos(replicaID, replicaClkValue)
				tm.mat.SendRequestToAllChannels(MaterializerRequest{MatRequestArgs: MatClkPosUpdArgs{ReplicaID: replicaID, StableTs: replicaClkValue}})
				tm.localClock.UpdatePos(replicaID, replicaClkValue)

				delete(tm.downstreamQueue, replicaID)
				delete(tm.downstreamClkQueue, replicaID)
			} else {
				tm.downstreamQueue[replicaID] = pendingTxns
			}
		}
	}
	fmt.Println("[TM", tm.replicaID, "]Finished checking for pending remote txns")
}

//TODO: Possibly cache the hashing results and return them? That would allow to include them in the requests and paralelize the read requests
func (tm *TransactionManager) findCommonTimestamp(objsParams []KeyParams, clientTs clocksi.Timestamp) (ts clocksi.Timestamp) {
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
			tm.mat.SendRequestToChannel(currTSRequest, currChanKey)
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
