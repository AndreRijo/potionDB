package antidote

import (
	fmt "fmt"
	"math/rand"
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
	"potionDB/src/proto"
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

//Counter

func (ic InternalClient) CounterIncUpd(value int32) crdt.Increment {
	return crdt.Increment{Change: value}
}

func (ic InternalClient) CounterDecUpd(value int32) crdt.Decrement {
	return crdt.Decrement{Change: value}
}

//Register

func (ic InternalClient) SetValueUpd(value interface{}) crdt.SetValue {
	return crdt.SetValue{NewValue: value}
}

//Set

func (ic InternalClient) SetAddUpd(element string) crdt.Add {
	return crdt.Add{Element: crdt.Element(element)}
}

func (ic InternalClient) SetRemUpd(element string) crdt.Remove {
	return crdt.Remove{Element: crdt.Element(element)}
}

func (ic InternalClient) SetAddAllUpd(toAdd []crdt.Element) crdt.AddAll {
	return crdt.AddAll{Elems: toAdd}
}

func (ic InternalClient) SetRemAllUpd(toRem []crdt.Element) crdt.RemoveAll {
	return crdt.RemoveAll{Elems: toRem}
}

func (ic InternalClient) SetLookup(element string) crdt.LookupReadArguments {
	return crdt.LookupReadArguments{Elem: crdt.Element(element)}
}

//ORMap
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

//RRMap
//Note: for removes use the operation from the ORMap. RWMap supports the same queries as ORMap and two new ones.

//InnerUpd is an update to a CRDT. E.g., Increment, MapAdd, etc.
func (ic InternalClient) RWMapUpdateUpd(key string, innerUpd crdt.UpdateArguments) crdt.EmbMapUpdate {
	return crdt.EmbMapUpdate{Key: key, Upd: innerUpd}
}

func (ic InternalClient) RWMapUpdateAllUpd(upds map[string]crdt.UpdateArguments) crdt.EmbMapUpdateAll {
	return crdt.EmbMapUpdateAll{Upds: upds}
}

//InnerReadArgs is a read to a CRDT. E.g., Lookup, GetKeys, etc. StateReadArguments can also be used
func (ic InternalClient) RWMapGetValue(key string, innerReadArgs crdt.ReadArguments) crdt.EmbMapGetValueArguments {
	return crdt.EmbMapGetValueArguments{Key: key, Args: innerReadArgs}
}

//For multiple keys
func (ic InternalClient) RWMapGetMultipleValues(args map[string]crdt.ReadArguments) crdt.EmbMapPartialArguments {
	return crdt.EmbMapPartialArguments{Args: args}
}

//TopK

//Note: data is optional
func (ic InternalClient) TopKAddUpd(id, score int32, data *[]byte) crdt.TopKAdd {
	return crdt.TopKAdd{TopKScore: ic.CreateTopKScore(id, score, data)}
}

func (ic InternalClient) TopKRemUpd(id int32) crdt.TopKRemove {
	return crdt.TopKRemove{Id: id}
}

//Auxiliary for TopKAddAll
func (ic InternalClient) CreateTopKScore(id, score int32, data *[]byte) crdt.TopKScore {
	return crdt.TopKScore{Id: id, Score: score, Data: data}
}

func (ic InternalClient) TopKAddAllUpd(scores []crdt.TopKScore) crdt.TopKAddAll {
	return crdt.TopKAddAll{Scores: scores}
}

func (ic InternalClient) TopKRmvAllUpd(ids []int32) crdt.TopKRemoveAll {
	return crdt.TopKRemoveAll{Ids: ids}
}

func (ic InternalClient) TopKGetTopN(nEntries int32) crdt.GetTopNArguments {
	return crdt.GetTopNArguments{NumberEntries: nEntries}
}

func (ic InternalClient) TopKGetTopAbove(minValue int32) crdt.GetTopKAboveValueArguments {
	return crdt.GetTopKAboveValueArguments{MinValue: minValue}
}

//Avg

func (ic InternalClient) AvgAddUpd(value int64) crdt.AddValue {
	return crdt.AddValue{Value: value}
}

func (ic InternalClient) AvgAddMultipleUpd(sumValue, nAdds int64) crdt.AddMultipleValue {
	return crdt.AddMultipleValue{SumValue: sumValue, NAdds: nAdds}
}

func (ic InternalClient) AvgGetFull() crdt.AvgGetFullArguments {
	return crdt.AvgGetFullArguments{}
}

//MaxMin

func (ic InternalClient) MaxMinAddMaxUpd(value int64) crdt.MaxAddValue {
	return crdt.MaxAddValue{Value: value}
}

func (ic InternalClient) MaxMinAddMinUpd(value int64) crdt.MinAddValue {
	return crdt.MinAddValue{Value: value}
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
