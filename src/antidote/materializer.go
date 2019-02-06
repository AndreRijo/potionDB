package antidote

import (
	"clocksi"
	"crdt"
	math "math"
	"tools"

	hashFunc "github.com/twmb/murmur3"
)

//TODO: This should prob be in lowercase to not expose externally
//This structure should be always created by using createDbKeyStruct, as it automatically generates dbKey
/*
type internalKey struct {
	KeyParams
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

/////*****************TYPE DEFINITIONS***********************/////

type MaterializerRequest struct {
	KeyParams
	Timestamp clocksi.Timestamp
	Args      MatRequestArgs
}

type MatRequestArgs interface {
	getRequestType() (requestType MatRequestType)
}

type MatReadArgs struct {
	ReplyChan chan crdt.State
}

type MatUpdateArgs struct {
	crdt.UpdateArguments
	ReplyChan chan bool //TODO: This probably will need to be changed to include error types
}

type MatRequestType int

//TODO: Typechecking...

/////*****************CONSTANTS AND VARIABLES***********************/////

const (
	//Types of requests
	readMatRequest  MatRequestType = 0
	writeMatRequest MatRequestType = 1
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
	requestType = readMatRequest
	return
}

func (args MatUpdateArgs) getRequestType() (requestType MatRequestType) {
	requestType = writeMatRequest
	return
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
	//Each goroutine is responsible for the range of keys [minKey, maxKey[
	//Calculates range of keys for current goroutine
	//factor := math.MaxUint64 / nGoRoutines
	//minKey, maxKey := factor*id, factor*(id+1)
	//minKey := factor * id
	//math.MaxUint64 / nGoRoutines might not be an integer, so that result * nGoRoutines might leave a few keys out of range
	/*
		if id+1 == nGoRoutines {
			maxKey = math.MaxUint64
		}
	*/
	//minKey := keyRangeSize * id
	//fmt.Println("(id, minkey)", id, minKey)
	//Listens to the channel and processes requests
	channel := make(chan MaterializerRequest)
	channels[id] = channel
	for {
		request := <-channel
		//fmt.Println("Materializer goroutine", id, "received a request!")
		handleMatRequest(request)
	}
}

func handleMatRequest(request MaterializerRequest) {
	switch request.Args.getRequestType() {
	case readMatRequest:
		handleMatRead(request)
	case writeMatRequest:
		handleMatWrite(request)
	}
}

func handleMatRead(request MaterializerRequest) {
	hashKey := getHash(getCombinedKey(request.KeyParams))

	obj, hasKey := db[hashKey]
	var state crdt.State
	if !hasKey {
		//TODO: Handle error as antidote does (check what it does? I think it just returns the object with the initial state)
		state = initializeCrdt(request.CrdtType).GetValue()
	} else {
		state = obj.GetValue()
	}

	request.Args.(MatReadArgs).ReplyChan <- state

}

func handleMatWrite(request MaterializerRequest) {
	hashKey := getHash(getCombinedKey(request.KeyParams))

	obj, hasKey := db[hashKey]
	if !hasKey {
		obj = initializeCrdt(request.CrdtType)
		db[hashKey] = obj
	}
	downstreamArgs := obj.Update(request.Args.(MatUpdateArgs).UpdateArguments)
	//TODO: Replicate the operation or store in list to replicate of current transaction...? Maybe return to caller...?
	//TODO: Also, error handling...?
	obj.Downstream(downstreamArgs)

	request.Args.(MatUpdateArgs).ReplyChan <- true
}

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

//Called by the TransactionManager or any other entity that may want to communicate with the Materializer.
func SendRequest(request MaterializerRequest) {
	channelKey := getChannelKey(request)
	channels[channelKey] <- request
}

func getChannelKey(request MaterializerRequest) (key uint64) {
	hash := getHash(getCombinedKey(request.KeyParams))
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
