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
//TODO: Probably change this to MatStaticUpdateArgs and make a different one for MatUpdateArgs... the difference would be only on ReplyChan.
type MatUpdateArgs struct {
	Updates []UpdateObjectParams
	TransactionId
	ReplyChan chan BoolErrorPair //TODO: This probably will need to be changed to include error types
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
	ChannelId       uint64
	TransactionId   TransactionId
	CommitTimestamp clocksi.Timestamp
}

//TODO: Stop using timestamp as transaction id (problem: client that sends two requests with same seen clock)
type MatStartTransactionArgs struct {
	ChannelId uint64
	TransactionId
	ReplyChan chan clocksi.Timestamp
}

type MatRequestType byte

//////////********************Other types************************//////////
//Struct that represents local data to each goroutine/partition
type partitionData struct {
	stableVersion          clocksi.Timestamp                      //latest commited timestamp
	smallestPendingVersion clocksi.Timestamp                      //the oldest timestamp that wasn't yet commited but has already been prepared
	highestVersionSeen     clocksi.Timestamp                      //Contains the highest timestamp seen (i.e., possibly not commited) so far
	pendingOps             map[TransactionId][]UpdateObjectParams //pending transactions waiting for commit
	suggestedTimestamps    map[TransactionId]clocksi.Timestamp    //map of transactionId -> timestamp suggested on first write request for transactionId
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

//TODO: Typechecking...

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
	//Number of goroutines in the pool to access the db. Each goroutine has a (automatically assigned) range of keys that it can access.
	nGoRoutines uint64 = 8
)

var (
	//db = make(map[combinedKey]crdt.CRDT)
	//uint64: result returned by the hash function
	db           = make(map[uint64]crdt.CRDT)
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
	return args.ChannelId
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
		stableVersion:          clocksi.ClockSiTimestamp{}.NewTimestamp(),
		smallestPendingVersion: nil,
		highestVersionSeen:     clocksi.ClockSiTimestamp{}.NewTimestamp(),
		pendingOps:             make(map[TransactionId][]UpdateObjectParams),
		suggestedTimestamps:    make(map[TransactionId]clocksi.Timestamp),
	}
	//Listens to the channel and processes requests
	channel := make(chan MaterializerRequest)
	channels[id] = channel
	for {
		request := <-channel
		handleMatRequest(request, &partitionData, id)
	}
}

func handleMatRequest(request MaterializerRequest, partitionData *partitionData, id uint64) {
	switch request.getRequestType() {
	case readMatRequest:
		handleMatRead(request, partitionData, id)
	case writeStaticMatRequest:
		handleMatStaticWrite(request, partitionData, id)
	case writeMatRequest:
		handleMatWrite(request, partitionData, id)
	case versionMatRequest:
		handleMatVersion(request, partitionData, id)
	case commitMatRequest:
		handleMatCommit(request, partitionData, id)
	}
}

func handleMatRead(request MaterializerRequest, partitionData *partitionData, id uint64) {
	//TODO: Actually take in consideration the timestamp to read the correct version
	readArgs := request.MatRequestArgs.(MatReadArgs)
	hashKey := getHash(getCombinedKey(readArgs.KeyParams))
	obj, hasKey := db[hashKey]
	var state crdt.State
	if !hasKey {
		//TODO: Handle error as antidote does (check what it does? I think it just returns the object with the initial state)
		state = initializeCrdt(readArgs.CrdtType).GetValue()
	} else {
		state = obj.GetValue()
	}

	readArgs.ReplyChan <- state
}

func handleStartTransaction(request MaterializerRequest, partitionData *partitionData, id uint64) {
	startTransArgs := request.MatRequestArgs.(MatStartTransactionArgs)
	auxiliaryStartTransaction(startTransArgs.TransactionId, partitionData)
	startTransArgs.ReplyChan <- partitionData.suggestedTimestamps[startTransArgs.TransactionId]
}

//Contains code shared between startTransaction and staticWrite
func auxiliaryStartTransaction(transactionId TransactionId, partitionData *partitionData) {
	newTimestamp := partitionData.highestVersionSeen.NextTimestamp()
	partitionData.highestVersionSeen = newTimestamp
	partitionData.suggestedTimestamps[transactionId] = newTimestamp
}

func handleMatStaticWrite(request MaterializerRequest, partitionData *partitionData, id uint64) {
	writeArgs := request.MatRequestArgs.(MatStaticUpdateArgs)

	auxiliaryStartTransaction(writeArgs.TransactionId, partitionData)

	ok, err := typecheckWrites(writeArgs.Updates)
	if !ok {
		//TODO: Changes here might be needed after implementing abort
		delete(partitionData.suggestedTimestamps, writeArgs.TransactionId)
		writeArgs.ReplyChan <- TimestampErrorPair{
			Timestamp: nil,
			error:     err,
		}
	} else {
		partitionData.pendingOps[writeArgs.TransactionId] = writeArgs.Updates
		writeArgs.ReplyChan <- TimestampErrorPair{
			Timestamp: partitionData.suggestedTimestamps[writeArgs.TransactionId],
			error:     nil,
		}
	}

}

func handleMatWrite(request MaterializerRequest, partitionData *partitionData, id uint64) {
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

func handleMatVersion(request MaterializerRequest, partitionData *partitionData, id uint64) {
	//request.MatRequestArgs.(MatVersionArgs).ReplyChan <- stableVersions[id]
	request.MatRequestArgs.(MatVersionArgs).ReplyChan <- partitionData.stableVersion
}

func handleMatCommit(request MaterializerRequest, partitionData *partitionData, id uint64) {
	commitArgs := request.MatRequestArgs.(MatCommitArgs)

	updates := partitionData.pendingOps[commitArgs.TransactionId]
	for _, upd := range updates {
		hashKey := getHash(getCombinedKey(upd.KeyParams))

		obj, hasKey := db[hashKey]
		if !hasKey {
			obj = initializeCrdt(upd.CrdtType)
			db[hashKey] = obj
		}
		downstreamArgs := obj.Update(upd)
		obj.Downstream(downstreamArgs)
	}

	//Transactions are commited in order, so this commit timestamp is always more recent than the previous stableVersion
	partitionData.stableVersion = commitArgs.CommitTimestamp
	//But we might have already prepared a more recent timestamp
	if commitArgs.CommitTimestamp.IsHigherOrEqual(partitionData.highestVersionSeen) {
		partitionData.highestVersionSeen = commitArgs.CommitTimestamp
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
