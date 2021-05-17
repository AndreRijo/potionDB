package antidote

import (
	"fmt"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"time"
)

func InitializeAdmin(tm *TransactionManager) {
	ic := InternalClient{}.Initialize(tm)
	AdminUpdate(ic, tm.replicator.getBuckets(), tm.replicaID)
	AdminRead(ic)
	time.Sleep(10 * time.Second)
	AdminRead(ic)
}

func AdminUpdate(client InternalClient, buckets []string, replicaID int16) {
	objId := CreateKeyParams("adminKey", proto.CRDTType_RRMAP, "admin")
	for _, b := range buckets {
		fmt.Printf("[ADMIN_MAP]: Bucket %s is replicated by replica %v\n", b, replicaID)
		op := client.RWMapUpdateUpd(b, client.SetAddUpd(strconv.FormatInt(int64(replicaID), 10)))
		client.DoSingleUpdate(objId, op)
	}
}

func AdminRead(client InternalClient) crdt.EmbMapEntryState {
	objId := CreateKeyParams("adminKey", proto.CRDTType_RRMAP, "admin")
	read := client.FullRead()
	state := client.DoSingleRead(objId, read)
	mapFullState := state.(crdt.EmbMapEntryState)
	fmt.Println("[ADMIN_MAP]: ", mapFullState.States)
	return mapFullState
}

func AdminReadByBucket(client InternalClient, bucket string) {
	readBuf := client.CreateReadBuf(1)
	objId := CreateKeyParams("adminKey", proto.CRDTType_RRMAP, "admin")
	readBuf = client.AddRead(objId, client.RWMapGetValue(bucket, client.FullRead()), readBuf)
	states := client.DoRead(readBuf)

	for _, state := range states {
		fmt.Println(state.(crdt.EmbMapGetValueState).State)
	}
}

func AdminReadByBucketList(client InternalClient, bucket string) {
	readBuf := client.CreateReadBuf(1)
	objId := CreateKeyParams("adminKey", proto.CRDTType_RRMAP, "admin")
	readBuf = client.AddRead(objId, client.RWMapGetValue(bucket, client.FullRead()), readBuf)
	states := client.DoRead(readBuf)

	replicas := make([]crdt.State, 0)
	for _, state := range states {
		replicas = append(replicas, state.(crdt.EmbMapGetValueState).State)
	}
	fmt.Println(replicas)
}
