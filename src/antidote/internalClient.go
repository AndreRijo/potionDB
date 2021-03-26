package antidote

import (
	"clocksi"
	"crdt"
	fmt "fmt"
	"math/rand"
	"proto"
)

const (
	UNKNOWN_SIZE = -1
)

//Note: Clients can be reused between operations... the idea is to only create the client
type InternalClient struct {
	tmChan chan TransactionManagerRequest
}

func (ic InternalClient) Initialize(tm *TransactionManager) InternalClient {
	ic.tmChan = tm.CreateClientHandler()
	return ic
}

//Note: the examples assume a client was already created like this: InternalClient{}.Initialize(tm)
//Where tm is the instance of TransactionManager

//For when you just want to execute one operation
func simpleUpdExample(client InternalClient) {
	//Define key and operation
	objId := CreateKeyParams("myKey", proto.CRDTType_ORMAP, "myBucket")
	op := client.MapAddUpd("key", "value")
	//Call client.DoSingleUpdate
	client.DoSingleUpdate(objId, op)
}

func simpleReadExample(client InternalClient) {
	//Define key and read
	objId := CreateKeyParams("myKey", proto.CRDTType_ORMAP, "myBucket")
	read := client.FullRead()
	//Call client.DoSingleRead
	state := client.DoSingleRead(objId, read)
	//Convert the generic state to the specific state. For the map, check orMap.go ("States returned by queries")
	mapFullState := state.(crdt.MapEntryState)
	fmt.Println(mapFullState.Values)
}

func exampleWrite(client InternalClient) {
	//Create a buffer with space for operations. If possible define a buffer with the exact size you'll use.
	//If you don't know apriori que size pass UNKNOWN_SIZE
	updBuf := client.CreateUpdBuf(1)
	//Initialize the triple (key, bucket, CRDTType that identifies an object).
	//You can modify multiple objects in a single transaction
	objId := CreateKeyParams("myKey", proto.CRDTType_ORMAP, "myBucket")
	//Call AddUpdate() with the arguments: objId, CRDT-specific operation, buffer
	updBuf = client.AddUpdate(objId, client.MapAddUpd("key", "value"), updBuf)
	//Add whichever extra updates you want to execute
	//...
	//Call DoUpdate()
	client.DoUpdate(updBuf)
}

func exampleRead(client InternalClient) {
	//Create a buffer with space for reads. If possible define a buffer with the exact size you'll use.
	//If you don't know apriori que size pass UNKNOWN_SIZE
	readBuf := client.CreateReadBuf(1)
	//Initialize the triple (key, bucket, CRDTType that identifies an object).
	//You can modify multiple objects in a single transaction
	objId := CreateKeyParams("myKey", proto.CRDTType_ORMAP, "myBucket")
	//Call AddRead() with the arguments: objId, read operation, buffer
	readBuf = client.AddRead(objId, client.FullRead(), readBuf)
	//Add whichever extra reads you want to execute
	//...
	//Call DoRead()
	states := client.DoRead(readBuf)
	//Iterate though the states, convert and do stuff with them
	for _, state := range states {
		fmt.Println((state.(crdt.MapEntryState)).Values)
	}
}

func (ic InternalClient) CreateUpdBuf(size int) []*UpdateObjectParams {
	if size <= 0 {
		return make([]*UpdateObjectParams, 0, 10)
	}
	return make([]*UpdateObjectParams, 0, size)
}

func (ic InternalClient) DoSingleUpdate(keyParams KeyParams, upd crdt.UpdateArguments) {
	ic.DoUpdate([]*UpdateObjectParams{&UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &upd}})
}

func (ic InternalClient) DoUpdate(updBuf []*UpdateObjectParams) {
	replyChan := make(chan TMStaticUpdateReply)
	ic.tmChan <- ic.createTMRequest(TMStaticUpdateArgs{UpdateParams: updBuf, ReplyChan: replyChan})
	<-replyChan //Awaits until the materializer's partitions confirm the transaction
	//At this point the update is done
}

func (ic InternalClient) AddUpdate(keyParams KeyParams, upd crdt.UpdateArguments, updBuf []*UpdateObjectParams) []*UpdateObjectParams {
	return append(updBuf, &UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &upd})
}

func (ic InternalClient) CreateReadBuf(size int) []ReadObjectParams {
	if size <= 0 {
		return make([]ReadObjectParams, 0, 10)
	}
	return make([]ReadObjectParams, 0, size)
}

func (ic InternalClient) DoSingleRead(keyParams KeyParams, read crdt.ReadArguments) crdt.State {
	return ic.DoRead([]ReadObjectParams{ReadObjectParams{KeyParams: keyParams, ReadArgs: read}})[0]
}

func (ic InternalClient) DoRead(readBuf []ReadObjectParams) []crdt.State {
	replyChan := make(chan TMStaticReadReply)
	ic.tmChan <- ic.createTMRequest(TMStaticReadArgs{ReadParams: readBuf, ReplyChan: replyChan})
	return (<-replyChan).States
}

func (ic InternalClient) AddRead(keyParams KeyParams, read crdt.ReadArguments, readBuf []ReadObjectParams) []ReadObjectParams {
	return append(readBuf, ReadObjectParams{KeyParams: keyParams, ReadArgs: read})
}

//

//CRDT specific operations
//Note: You can add more of those operations if you need for another CRDT. Just follow the examples :)

//This read works for every CRDT. It is the "default" reading operation. E.g., in a map, it returns all the mappings.
func (ic InternalClient) FullRead() crdt.StateReadArguments {
	return crdt.StateReadArguments{}
}

//MAP
func (ic InternalClient) MapAddUpd(key, element string) crdt.MapAdd {
	return crdt.MapAdd{Key: key, Value: crdt.Element(element)}
}

func (ic InternalClient) MapRemUpd(key string) crdt.MapRemove {
	return crdt.MapRemove{Key: key}
}

func (ic InternalClient) MapMultipleAddUpd(toAdd map[string]crdt.Element) crdt.MapAddAll {
	return crdt.MapAddAll{Values: toAdd}
}

func (ic InternalClient) MapMultipleRemUpd(keys []string) crdt.MapRemoveAll {
	return crdt.MapRemoveAll{Keys: keys}
}

func (ic InternalClient) MapGetValueRead(key string) crdt.GetValueArguments {
	return crdt.GetValueArguments{Key: key}
}

func (ic InternalClient) MapHasKeyRead(key string) crdt.HasKeyArguments {
	return crdt.HasKeyArguments{Key: key}
}

func (ic InternalClient) MapGetKeys() crdt.GetKeysArguments {
	return crdt.GetKeysArguments{}
}

//Read multiple keys
func (ic InternalClient) MapGetValues(keys []string) crdt.GetValuesArguments {
	return crdt.GetValuesArguments{Keys: keys}
}

//

//Internal/auxiliary methods.

func (ic InternalClient) createTMRequest(args TMRequestArgs) (request TransactionManagerRequest) {
	return TransactionManagerRequest{
		Args:          args,
		TransactionId: TransactionId(rand.Uint64()),
		Timestamp:     clocksi.NewClockSiTimestamp(), //This way the TM will use the latest clock available
	}
}
