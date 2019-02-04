package antidote

//TODO: Try refactoring this to be inside another folder in antidote
//(e.g: antidote/transaction), and check if that still has circular import issues

import (
	"clocksi"
	"crdt"
)

type KeyParams struct {
	Key      string
	CrdtType CRDTType
	Bucket   string
}

type ReadObjectParams struct {
	KeyParams //This grants access to any of the fields in KeyParams
}

type UpdateObjectParams struct {
	KeyParams
	UpdateArgs crdt.UpdateArguments
}

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
			Args: ReadRequestArgs{
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
			Args: UpdateRequestArgs{
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
