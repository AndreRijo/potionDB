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
	Timestamp clocksi.Timestamp
	Args      TMRequestArgs
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

/////*****************CONSTANTS AND VARIABLES***********************/////

const (
	readTMRequest   TMRequestType = 0
	writeTMRequest  TMRequestType = 1
	lostConnRequest TMRequestType = 255
)

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

//For now ignore the client's timestamp
//TODO: Actually take in consideration the client's timestamp
func handleTMRead(request TransactionManagerRequest) {
	ts := clocksi.NextTimestamp()
	readRequest := request.Args.(TMReadArgs)
	states := make([]crdt.State, len(readRequest.ObjsParams))
	//TODO: Couple this in a single request?
	replyChan := make(chan crdt.State)

	for i, obj := range readRequest.ObjsParams {
		request := MaterializerRequest{
			KeyParams: obj,
			Args: MatReadArgs{
				ReplyChan: replyChan,
			},
			Timestamp: request.Timestamp,
		}
		SendRequest(request)
		states[i] = <-replyChan
	}
	close(replyChan)

	readRequest.ReplyChan <- TMReadReply{
		States:    states,
		Timestamp: ts,
	}
}

func handleTMWrite(request TransactionManagerRequest) {
	ts := clocksi.NextTimestamp()
	writeRequest := request.Args.(TMUpdateArgs)
	//TODO: Couple this in a single request?
	replyChan := make(chan bool)

	for _, upd := range writeRequest.UpdateParams {
		request := MaterializerRequest{
			KeyParams: upd.KeyParams,
			Args: MatUpdateArgs{
				UpdateArguments: upd.UpdateArgs,
				ReplyChan:       replyChan,
			},
			Timestamp: request.Timestamp,
		}
		SendRequest(request)
		<-replyChan
	}
	close(replyChan)

	writeRequest.ReplyChan <- TMUpdateReply{
		Timestamp: ts,
		Err:       nil,
	}
}

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

//This is here just in case for some reason I decide to go back to this
/*
//Interface that contains the necessary information to identify an object
type ObjectLocation interface {
	GetKey() (key string)
	GetCRDTType() (crdtType CRDTType)
	GetBucket() (bucket string)
}

func (params ReadObjectParams) GetKey() (key string) { key = params.Key; return }

func (params ReadObjectParams) GetCRDTType() (crdtType CRDTType) { crdtType = params.CrdtType; return }

func (params ReadObjectParams) GetBucket() (bucket string) { bucket = params.Bucket; return }

func (params UpdateObjectParams) GetKey() (key string) { key = params.Key; return }

func (params UpdateObjectParams) GetCRDTType() (crdtType CRDTType) { crdtType = params.CrdtType; return }

func (params UpdateObjectParams) GetBucket() (bucket string) { bucket = params.Bucket; return }

*/
