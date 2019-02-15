package antidote

import (
	"clocksi"
	"crdt"
	fmt "fmt"
	math "math"

	hashFunc "github.com/twmb/murmur3"
)

/////*****************TYPE DEFINITIONS***********************/////

//////////************Requests**************************//////////

type MaterializerRequest struct {
	MatRequestArgs
}

type MatRequestArgs interface {
	getRequestType() (requestType MatRequestType)
	getChannel() (channelId uint64)
}

//Args for read request
type MatReadArgs struct {
	KeyParams
	clocksi.Timestamp
	ReplyChan chan crdt.State
}

//Args for update request. Note that unlike with MatReadArgs, a MatUpdateArgs represents multiple updates, but all for the same partition
type MatUpdateArgs struct {
	Updates []UpdateObjectParams
	TransactionId
	ReplyChan chan BoolErrorPair
}

type MatStaticUpdateArgs struct {
	Updates []UpdateObjectParams
	TransactionId
	ReplyChan chan TimestampErrorPair
}

//Args for latest stable version request.
type MatVersionArgs struct {
	ChannelId uint64
	ReplyChan chan clocksi.Timestamp
}

type MatCommitArgs struct {
	TransactionId   TransactionId
	CommitTimestamp clocksi.Timestamp
}

type MatStartTransactionArgs struct {
	ChannelId uint64
	TransactionId
	ReplyChan chan clocksi.Timestamp
}

type MatRequestType byte

//////////********************Other types************************//////////
//Struct that represents local data to each goroutine/partition
type partitionData struct {
	db                     map[uint64]crdt.CRDT                   //CRDT database of this partition
	stableVersion          clocksi.Timestamp                      //latest commited timestamp
	twoSmallestPendingTxn  [2]*TransactionId                       //Contains the two transactionIds that have been prepared with the smallest timestamps.
																  //Idea: avoids the issue of the txn we're verying being the one with the lowest proposed timestamp (in this case, check the 2nd entry)
	highestPendingTs       clocksi.Timestamp                      //Contains the highest timestamp that was prepared. Used to check if a read can be executed or not.
	pendingOps             map[TransactionId][]UpdateObjectParams //pending transactions waiting for commit
	suggestedTimestamps    map[TransactionId]clocksi.Timestamp    //map of transactionId -> timestamp suggested on first write request for transactionId
	commitedWaitToApply    map[TransactionId]clocksi.Timestamp    //set of transactionId -> commit timestamp of commited transactions that couldn't be applied due to pending versions
	//TODO: Choose a better option to hold pending reads? Checking the whole map takes a long time...
	pendingReads map[clocksi.Timestamp][]*MatReadArgs //pending reads that require a more recent version than stableVersion
}

type BoolErrorPair struct {
	bool
	error
}

type TimestampErrorPair struct {
	clocksi.Timestamp
	error
}

//////////*******************Error types***********************//////////
//TODO: Move this to crdt.go...?
type UnknownCrdtTypeError struct {
	CRDTType
}

/////*****************CONSTANTS AND VARIABLES***********************/////

const (
	//Types of requests
	readMatRequest             MatRequestType = 0
	writeStaticMatRequest      MatRequestType = 1
	writeMatRequest            MatRequestType = 2
	versionMatRequest          MatRequestType = 3
	startTransactionMatRequest MatRequestType = 4
	commitMatRequest           MatRequestType = 5
	//TODO: Maybe each bucket should correspond to one goroutine...?
	//Number of goroutines in the pool to access the database. Each goroutine has a (automatically assigned) range of keys that it can access.
	nGoRoutines   uint64 = 8
	readQueueSize        = 10 //Initial size of the read queue for pending reads (partitionData.pendingReads)
)

var (
	//uint64: result returned by the hash function
	keyRangeSize uint64 //Number of keys that each goroutine is responsible, except for the last one which might have a bit more.
	//Each goroutine is responsible for a certain range of keys (with no intersection between ranges)
	//More precisely, a goroutine is responsible from its id * keyRangeSize (inclusive) to (id + 1) * keyRangeSize (exclusive)
	channels = make([]chan MaterializerRequest, nGoRoutines)
)

/////*****************TYPE METHODS***********************/////

func (args MatReadArgs) getRequestType() (requestType MatRequestType) {
	return readMatRequest
}
func (args MatReadArgs) getChannel() (channelId uint64) {
	return GetChannelKey(args.KeyParams)
}

func (args MatStaticUpdateArgs) getRequestType() (requestType MatRequestType) {
	return writeStaticMatRequest
}
func (args MatStaticUpdateArgs) getChannel() (channelId uint64) {
	return GetChannelKey(args.Updates[0].KeyParams)
}

func (args MatUpdateArgs) getRequestType() (requestType MatRequestType) {
	return writeMatRequest
}
func (args MatUpdateArgs) getChannel() (channelId uint64) {
	return GetChannelKey(args.Updates[0].KeyParams)
}

func (args MatVersionArgs) getRequestType() (requestType MatRequestType) {
	return versionMatRequest
}
func (args MatVersionArgs) getChannel() (channelId uint64) {
	return args.ChannelId
}

func (args MatStartTransactionArgs) getRequestType() (requestType MatRequestType) {
	return startTransactionMatRequest
}
func (args MatStartTransactionArgs) getChannel() (channelId uint64) {
	return args.ChannelId
}

func (args MatCommitArgs) getRequestType() (requestType MatRequestType) {
	return commitMatRequest
}

func (args MatCommitArgs) getChannel() (channelId uint64) {
	return 0 //Commits are supposed to be sent to all partitions
}

func (err UnknownCrdtTypeError) Error() (errString string) {
	return fmt.Sprint("Unknown/unsupported CRDT type:", err.CRDTType)
}

/////*****************MATERIALIZER CODE***********************/////

//Starts listening goroutines and channels
func InitializeMaterializer() {
	keyRangeSize = math.MaxUint64 / nGoRoutines
	var i uint64
	for i = 0; i < nGoRoutines; i++ {
		go listenForTransactionManagerRequests(i)
	}
}

func listenForTransactionManagerRequests(id uint64) {
	//Each goroutine is responsible for the range of keys [keyRangeSize * id, keyRangeSize * (id + 1)[
	//Where keyRangeSize = math.MaxUint64 / number of goroutines

	partitionData := partitionData{
		db:                     make(map[uint64]crdt.CRDT),
		stableVersion:          clocksi.ClockSiTimestamp{}.NewTimestamp(),
		highestPendingTs:       nil,
		pendingOps:             make(map[TransactionId][]UpdateObjectParams),
		suggestedTimestamps:    make(map[TransactionId]clocksi.Timestamp),
		commitedWaitToApply:    make(map[TransactionId]clocksi.Timestamp),
		pendingReads:           make(map[clocksi.Timestamp][]*MatReadArgs),
	}
	//Listens to the channel and processes requests
	channel := make(chan MaterializerRequest)
	channels[id] = channel
	for {
		request := <-channel
		handleMatRequest(request, &partitionData)
	}
}

func handleMatRequest(request MaterializerRequest, partitionData *partitionData) {
	switch request.getRequestType() {
	case readMatRequest:
		handleMatRead(request, partitionData)
	case writeStaticMatRequest:
		handleMatStaticWrite(request, partitionData)
	case writeMatRequest:
		handleMatWrite(request, partitionData)
	case versionMatRequest:
		handleMatVersion(request, partitionData)
	case commitMatRequest:
		handleMatCommit(request, partitionData)
	}
}

func handleMatRead(request MaterializerRequest, partitionData *partitionData) {
	//TODO: Actually take in consideration the timestamp to read the correct version
	readArgs := request.MatRequestArgs.(MatReadArgs)
	if canRead(readArgs.Timestamp, partitionData) {
		applyReadAndReply(&readArgs, partitionData)
	} else {
		//Queue the request.
		queue, exists := partitionData.pendingReads[readArgs.Timestamp]
		if !exists {
			queue = make([]*MatReadArgs, 0, readQueueSize)
		}
		partitionData.pendingReads[readArgs.Timestamp] = append(queue, &readArgs)
	}
}

func canRead(readTs clocksi.Timestamp, partitionData *partitionData) (canRead bool) {
	if readTs.IsLowerOrEqual(partitionData.stableVersion) {
		canRead = true
	} else if partitionData.twoSmallestPendingTxn[0] != nil && 
		partitionData.suggestedTimestamps[*partitionData.twoSmallestPendingTxn[0]].IsLower(readTs) {
		//There's a commit prepared with a timestamp lower than read's
		canRead = false
	} else {
		localTs := clocksi.NewClockSiTimestamp().NextTimestamp()
		if (localTs.IsHigherOrEqual(readTs)) {
			canRead = true
		}
	}
	return
}

func applyReadAndReply(readArgs *MatReadArgs, partitionData *partitionData) {
	hashKey := getHash(getCombinedKey(readArgs.KeyParams))
	obj, hasKey := partitionData.db[hashKey]
	var state crdt.State
	if !hasKey {
		//TODO: Handle error as antidote does (check what it does? I think it just returns the object with the initial state)
		state = initializeCrdt(readArgs.CrdtType).GetValue()
	} else {
		state = obj.GetValue()
	}

	readArgs.ReplyChan <- state
}

func handleStartTransaction(request MaterializerRequest, partitionData *partitionData) {
	startTransArgs := request.MatRequestArgs.(MatStartTransactionArgs)
	auxiliaryStartTransaction(startTransArgs.TransactionId, partitionData)
	startTransArgs.ReplyChan <- partitionData.suggestedTimestamps[startTransArgs.TransactionId]
}

//Contains code shared between startTransaction and staticWrite
func auxiliaryStartTransaction(transactionId TransactionId, partitionData *partitionData) {
	var newTimestamp clocksi.Timestamp
	if (partitionData.highestPendingTs == nil) {
		newTimestamp = partitionData.stableVersion.NextTimestamp()
	} else {
		newTimestamp = partitionData.highestPendingTs.NextTimestamp()
	}
	partitionData.highestPendingTs = newTimestamp
	partitionData.suggestedTimestamps[transactionId] = newTimestamp
	if partitionData.twoSmallestPendingTxn[0] == nil {
		partitionData.twoSmallestPendingTxn[0] = &transactionId
	} else if partitionData.twoSmallestPendingTxn[1] == nil {
		partitionData.twoSmallestPendingTxn[1] = &transactionId
	}
}

func handleMatStaticWrite(request MaterializerRequest, partitionData *partitionData) {
	writeArgs := request.MatRequestArgs.(MatStaticUpdateArgs)

	ok, err := typecheckWrites(writeArgs.Updates)
	if !ok {
		writeArgs.ReplyChan <- TimestampErrorPair{
			Timestamp: nil,
			error:     err,
		}
	} else {
		auxiliaryStartTransaction(writeArgs.TransactionId, partitionData)
		partitionData.pendingOps[writeArgs.TransactionId] = writeArgs.Updates
		writeArgs.ReplyChan <- TimestampErrorPair{
			Timestamp: partitionData.suggestedTimestamps[writeArgs.TransactionId],
			error:     nil,
		}
	}

}

func handleMatWrite(request MaterializerRequest, partitionData *partitionData) {
	writeArgs := request.MatRequestArgs.(MatUpdateArgs)

	ok, err := typecheckWrites(writeArgs.Updates)
	if ok {
		partitionData.pendingOps[writeArgs.TransactionId] = append(partitionData.pendingOps[writeArgs.TransactionId], writeArgs.Updates...)
	}
	writeArgs.ReplyChan <- BoolErrorPair{
		bool:  ok,
		error: err,
	}
}

func typecheckWrites(updates []UpdateObjectParams) (ok bool, err error) {
	//Typechecking
	for _, upd := range updates {
		tmpCrdt := initializeCrdt(upd.CrdtType)
		if tmpCrdt == nil {
			return false, UnknownCrdtTypeError{CRDTType: upd.CrdtType}
		}
		ok, err = tmpCrdt.IsOperationWellTyped(upd.UpdateArgs)
		if !ok {
			return
		}
	}

	return
}

func handleMatVersion(request MaterializerRequest, partitionData *partitionData) {
	request.MatRequestArgs.(MatVersionArgs).ReplyChan <- partitionData.stableVersion
}

/*
	Note: This is received by every partition even if no operations are targeted to some partitions.
	This is because every partition needs to know the latest version commited, so that reads are applied succesfully
*/
func handleMatCommit(request MaterializerRequest, partitionData *partitionData) {
	commitArgs := request.MatRequestArgs.(MatCommitArgs)

	if canCommit(commitArgs, partitionData) {
		//Safe to commit
		applyCommit(&commitArgs.TransactionId, &commitArgs.CommitTimestamp, partitionData)
	} else {
		//A transaction with smaller version is pending, so we need to queue this commit.
		partitionData.commitedWaitToApply[commitArgs.TransactionId] = commitArgs.CommitTimestamp
	}
}

func canCommit(commitArgs MatCommitArgs, partitionData *partitionData) (canCommit bool) {
	if (commitArgs.TransactionId != *partitionData.twoSmallestPendingTxn[0]) {
		canCommit = commitArgs.CommitTimestamp.IsLower(partitionData.suggestedTimestamps[*partitionData.twoSmallestPendingTxn[0]]) 
	} else {
		//The txn we're verying is the one for which we proposed the lowest value. Check the 2nd lowest.
		canCommit = partitionData.twoSmallestPendingTxn[1] == nil || commitArgs.CommitTimestamp.IsLower(partitionData.suggestedTimestamps[*partitionData.twoSmallestPendingTxn[1]])
	}
	return
	/*
	for txnId, ts := range partitionData.suggestedTimestamps {
		if txnId != commitArgs.TransactionId && ts.IsLower(commitArgs.CommitTimestamp) {
			return false
		}
	}
	return true
	*/
}

func applyCommit(transactionId *TransactionId, commitTimestamp *clocksi.Timestamp, partitionData *partitionData) {
	applyUpdates(partitionData.pendingOps[*transactionId], partitionData)

	updatePartitionDataWithCommit(transactionId, commitTimestamp, partitionData)
}

func updatePartitionDataWithCommit(transactionId *TransactionId, commitTimestamp *clocksi.Timestamp, partitionData *partitionData) {
	deleteTransactionMetadata(transactionId, partitionData)
	//Transactions are commited in order, so this commit timestamp is always more recent than the previous stableVersion
	partitionData.stableVersion = *commitTimestamp
	handlePendingCommits(partitionData)
}

func deleteTransactionMetadata(transactionId *TransactionId, partitionData *partitionData) {
	delete(partitionData.pendingOps, *transactionId)
	delete(partitionData.suggestedTimestamps, *transactionId)
	delete(partitionData.commitedWaitToApply, *transactionId)
}

func handlePendingCommits(partitionData *partitionData) {
	//First step: update twoSmallestPendingTxn
	//Note that since commits are in order, the twoSmallestPendingTxn[0] was always the latest commit applied.
	partitionData.twoSmallestPendingTxn[0] = partitionData.twoSmallestPendingTxn[1]
	partitionData.twoSmallestPendingTxn[1] = nil
	//No further commits pending
	if len(partitionData.suggestedTimestamps) == 0 {
		partitionData.highestPendingTs = nil
	} else if len(partitionData.suggestedTimestamps) > 1 {
		//Search for the now second smallest pending timestamp
		var smallestTs clocksi.Timestamp = partitionData.highestPendingTs
		for transId, ts := range partitionData.suggestedTimestamps {
			if ts.IsLowerOrEqual(smallestTs) {
				partitionData.twoSmallestPendingTxn[1] = &transId
				smallestTs = ts
			}
		}
	}
	//Second step: check if the smallest version corresponds to a commited transaction that wasn't yet applied
	if (partitionData.twoSmallestPendingTxn[0] != nil) {
		nextCommitTs, isWaitingToApply := partitionData.commitedWaitToApply[*partitionData.twoSmallestPendingTxn[0]]
		if isWaitingToApply {
			//Third step: apply that transaction
			applyCommit(partitionData.twoSmallestPendingTxn[0], &nextCommitTs, partitionData)
		}
	}
	//Third step: apply pending reads
	if len(partitionData.pendingReads) > 0 {
		applyPendingReads(partitionData)
	}
	return
}

func applyUpdates(updates []UpdateObjectParams, partitionData *partitionData) {
	for _, upd := range updates {
		hashKey := getHash(getCombinedKey(upd.KeyParams))

		obj, hasKey := partitionData.db[hashKey]
		if !hasKey {
			obj = initializeCrdt(upd.CrdtType)
			partitionData.db[hashKey] = obj
		}
		downstreamArgs := obj.Update(upd.UpdateArgs)
		obj.Downstream(downstreamArgs)
	}
}

func applyPendingReads(partitionData *partitionData) {
	for ts, readSlices := range partitionData.pendingReads {
		if canRead(ts, partitionData) {
			//Apply all reads of that transaction
			for _, readArgs := range readSlices {
				applyReadAndReply(readArgs, partitionData)
			}
			delete(partitionData.pendingReads, ts)
		}
	}
}

//TODO: Think of some better way of doing this...? This probably shouldn't even be here, but putting it in crdt.go creates a circular dependency
//(Maybe it should be in the same package...)
func initializeCrdt(crdtType CRDTType) (newCrdt crdt.CRDT) {
	switch crdtType {
	case CRDTType_COUNTER:
		newCrdt = (&crdt.CounterCrdt{}).Initialize()
	case CRDTType_ORSET:
		newCrdt = (&crdt.SetAWCrdt{}).Initialize()
	default:
		newCrdt = nil
	}
	return
}

/*
Called by the TransactionManager or any other entity that may want to communicate with the Materializer
and doesn't yet know the appropriate channel
*/
func SendRequest(request MaterializerRequest) {
	channels[request.getChannel()] <- request
}

/*
Called by the TransactionManager or any other entity that may want to communicate with the Materializer
when they know the appropriate channel. Avoids computing an extra hash.
*/
func SendRequestToChannel(request MaterializerRequest, channelKey uint64) {
	channels[channelKey] <- request
}

func SendRequestToAllChannels(request MaterializerRequest) {
	for _, channel := range channels {
		//Copy the request to avoid concurrent access problems
		newReq := request
		channel <- newReq
	}
}

func GetChannelKey(keyParams KeyParams) (key uint64) {
	hash := getHash(getCombinedKey(keyParams))
	key = hash / keyRangeSize
	//Overflow, which might happen due to rounding
	if key == nGoRoutines {
		key -= 1
	}
	return
}

func getCombinedKey(keyParams KeyParams) (combKey string) {
	combKey = keyParams.Bucket + keyParams.CrdtType.String() + keyParams.Key
	return
}

func getHash(combKey string) (hash uint64) {
	hash = hashFunc.StringSum64(combKey)
	return
}

//Old code before goroutines
//This structure should be always created by using createDbKeyStruct, as it automatically generates dbKey
/*
type internalKey struct {
	keyParams
	dbKey    combinedKey //auto generated. The result is stored to avoid repeating unecessary computation
}

func (keyStruct *internalKey) generateDbKey() {
	keyStruct.dbKey = combinedKey(keyStruct.bucket + keyStruct.crdtType.String() + keyStruct.key)
}

func createDbKeyStruct(key string, crdtType CRDTType, bucket string) (keyStruct internalKey) {
	keyStruct = internalKey{
		key:      key,
		crdtType: crdtType,
		bucket:   bucket,
	}
	keyStruct.generateDbKey()
	return
}


*/
/*
func ReadObject(key string, crdtType CRDTType, bucket string, timestamp clocksi.Timestamp) (state crdt.State) {
	hashKey := getHash(getCombinedKey(key, crdtType, bucket))

	obj, hasKey := db[hashKey]
	if !hasKey {
		//TODO: Handle error as antidote does (check what it does? I think it just returns the object with the initial state)
		state = initializeCrdt(crdtType).GetValue()
	} else {
		state = obj.GetValue()
	}

	return
}
*/

/*
func UpdateObject(key string, crdtType CRDTType, bucket string, opArgs crdt.UpdateArguments, timestamp clocksi.Timestamp) {
	//TODO: typechecking (check if opArgs is valid for the CRDT it is being applied to)
	hashKey := getHash(getCombinedKey(key, crdtType, bucket))

	obj, hasKey := db[hashKey]
	if !hasKey {
		obj = initializeCrdt(crdtType)
		db[hashKey] = obj
	}
	downstreamArgs := obj.Update(opArgs)
	//TODO: Replicate the operation or store in list to replicate of current transaction...? Maybe return to caller...?
	obj.Downstream(downstreamArgs)
}
*/
