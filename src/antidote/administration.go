package antidote

import (
	"fmt"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"time"
)

var ic InternalClient

func InitializeAdmin(tm *TransactionManager) {
	ic = InternalClient{}.Initialize(tm)
	AdminUpdate(tm.replicator.getBuckets(), tm.replicaID)
	AdminRead()
	time.Sleep(10 * time.Second)
	AdminRead()
}

func AdminUpdate(buckets []string, replicaID int16) {
	objId := CreateKeyParams("adminKey", proto.CRDTType_RRMAP, "admin")
	for _, b := range buckets {
		fmt.Printf("[ADMIN_MAP]: Bucket %s is replicated by replica %v\n", b, replicaID)
		op := ic.RWMapUpdateUpd(b, ic.SetAddUpd(strconv.FormatInt(int64(replicaID), 10)))
		ic.DoSingleUpdate(objId, op)
	}
}

func AdminRead() crdt.EmbMapEntryState {
	objId := CreateKeyParams("adminKey", proto.CRDTType_RRMAP, "admin")
	read := ic.FullRead()
	state := ic.DoSingleRead(objId, read)
	mapFullState := state.(crdt.EmbMapEntryState)
	fmt.Println("[ADMIN_MAP]: ", mapFullState.States)
	return mapFullState
}

func PrintMap(adminmap crdt.EmbMapEntryState, replicaID int16) {
	admin := adminmap.States
	replicas := admin["*"].(crdt.SetAWValueState).Elems
	fmt.Println(contains(replicas, strconv.FormatInt(int64(replicaID), 10)))
	fmt.Println(contains(replicas, "11111"))
	for j, rep := range replicas {
		fmt.Println(j, rep)
	}

}

func AdminReadByBucket(bucket string) []crdt.EmbMapGetValueState {
	readBuf := ic.CreateReadBuf(1)
	objId := CreateKeyParams("adminKey", proto.CRDTType_RRMAP, "admin")
	readBuf = ic.AddRead(objId, ic.RWMapGetValue(bucket, ic.FullRead()), readBuf)
	states := ic.DoRead(readBuf)
	var values []crdt.EmbMapGetValueState
	for _, state := range states {
		fmt.Println(state.(crdt.EmbMapGetValueState).State)
		values = append(values, state.(crdt.EmbMapGetValueState))
	}
	return values
}

func AdminReadByBucketList(bucket string) {
	readBuf := ic.CreateReadBuf(1)
	objId := CreateKeyParams("adminKey", proto.CRDTType_RRMAP, "admin")
	readBuf = ic.AddRead(objId, ic.RWMapGetValue(bucket, ic.FullRead()), readBuf)
	states := ic.DoRead(readBuf)

	replicas := make([]crdt.State, 0)
	for _, state := range states {
		replicas = append(replicas, state.(crdt.EmbMapGetValueState).State)
	}
	fmt.Println(replicas)
}
