package antidote

import (
	"clocksi"
	"crdt"
	math "math"
	"tools"

	hashFunc "github.com/twmb/murmur3"
)

/////*****************TYPE DEFINITIONS***********************/////

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
	Timestamp clocksi.Timestamp
	ReplyChan chan crdt.State
}

//Args for update request
type MatUpdateArgs struct {
	UpdateObjectParams
	Timestamp clocksi.Timestamp
	ReplyChan chan bool //TODO: This probably will need to be changed to include error types
}

//Args for latest stable version request.
type MatVersionArgs struct {
	ChannelId uint64
	ReplyChan chan clocksi.Timestamp
}

type MatRequestType byte

//TODO: Typechecking...

/////*****************CONSTANTS AND VARIABLES***********************/////

const (
	//Types of requests
	readMatRequest    MatRequestType = 0
	writeMatRequest   MatRequestType = 1
	versionMatRequest MatRequestType = 2
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
	//Contains the version/timestamp of the last successful commit in each partition
	stableVersions = make([]clocksi.Timestamp, nGoRoutines)
	//Contains the smallest version/timestamp of pending writes in each partition
	pendingVersions = make([]clocksi.Timestamp, nGoRoutines)
)

/////*****************TYPE METHODS***********************/////

func (args MatReadArgs) getRequestType() (requestType MatRequestType) {
	return readMatRequest
}

func (args MatReadArgs) getChannel() (channelId uint64) {
	return GetChannelKey(args.KeyParams)
}

func (args MatUpdateArgs) getRequestType() (requestType MatRequestType) {
	return writeMatRequest
}

func (args MatUpdateArgs) getChannel() (channelId uint64) {
	return GetChannelKey(args.KeyParams)
}

func (args MatVersionArgs) getRequestType() (requestType MatRequestType) {
	return versionMatRequest
}

func (args MatVersionArgs) getChannel() (channelId uint64) {
	return args.ChannelId
}

/////*****************MATERIALIZER CODE***********************/////

//Starts listening goroutines and channels
func InitializeMaterializer() {
	keyRangeSize = math.MaxUint64 / nGoRoutines
	var i uint64
	for i = 0; i < nGoRoutines; i++ {
		stableVersions[i] = clocksi.ClockSiTimestamp{}.NewTimestamp()
		//TODO: Discover if each entry of pendingVersions is initialized as nil
		go listenForTransactionManagerRequests(i)
	}
}

func listenForTransactionManagerRequests(id uint64) {
	//Each goroutine is responsible for the range of keys [keyRangeSize * id, keyRangeSize * (id + 1)[
	//Where keyRangeSize = math.MaxUint64 / number of goroutines

	//Listens to the channel and processes requests
	channel := make(chan MaterializerRequest)
	channels[id] = channel
	for {
		request := <-channel
		handleMatRequest(request, id)
	}
}

func handleMatRequest(request MaterializerRequest, id uint64) {
	switch request.getRequestType() {
	case readMatRequest:
		handleMatRead(request, id)
	case writeMatRequest:
		handleMatWrite(request, id)
	case versionMatRequest:
		handleMatVersion(request, id)
	}
}

func handleMatRead(request MaterializerRequest, id uint64) {
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

func handleMatWrite(request MaterializerRequest, id uint64) {
	writeArgs := request.MatRequestArgs.(MatUpdateArgs)
	hashKey := getHash(getCombinedKey(writeArgs.UpdateObjectParams.KeyParams))

	obj, hasKey := db[hashKey]
	if !hasKey {
		obj = initializeCrdt(writeArgs.CrdtType)
		db[hashKey] = obj
	}
	downstreamArgs := obj.Update(writeArgs.UpdateObjectParams)
	//TODO: Replicate the operation or store in list to replicate of current transaction...? Maybe return to caller...?
	//TODO: Also, error handling...?
	obj.Downstream(downstreamArgs)

	writeArgs.ReplyChan <- true
}

func handleMatVersion(request MaterializerRequest, id uint64) {
	request.MatRequestArgs.(MatVersionArgs).ReplyChan <- stableVersions[id]
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
		//TODO: Support other crdtTypes
		tools.CheckErr("Unsupported crdtType to initialize CRDT.", nil)
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
