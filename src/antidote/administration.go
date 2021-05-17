package antidote

import (
	"fmt"
	"net"
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"potionDB/src/tools"
	"reflect"
	"strconv"
	"time"
)

type ReadRemotePair struct {
	readObjs ReadObjectParams
	index    int
}

type State struct {
	remoteWrites  map[string][]UpdateObjectParams
	rmtUpdOnGoing map[string][]UpdateObjectParams
	rmtTxn        map[string][]byte
}

var (
	ic        InternalClient
	replicaID int16
	adminMap  map[string]crdt.State
	repIP     map[string]crdt.Element
	remoteTS  map[string]clocksi.Timestamp
	state     map[TransactionId]State
	conns     map[string]net.Conn
)

func InitializeAdmin(tm *TransactionManager) {
	ic = InternalClient{}.Initialize(tm)
	remoteTS = make(map[string]clocksi.Timestamp)
	conns = make(map[string]net.Conn)
	state = make(map[TransactionId]State)

	replicaID = tm.replicaID
	AdminUpdate(tm.replicator.getBuckets(), tm.replicaID)
	RepIPUpdate(tm.replicaID)
	time.Sleep(5 * time.Second)
	for {
		adminMap = AdminRead().States
		repIP = RepIPRead().Values
		time.Sleep(30 * time.Second)
	}
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
	res := reflect.DeepEqual(mapFullState.States, adminMap)
	if !res {
		fmt.Println("[ADMIN_MAP]: ", mapFullState.States)
	}
	return mapFullState
}

func RepIPUpdate(replicaID int16) {
	objId := CreateKeyParams("repIPKey", proto.CRDTType_ORMAP, "admin")
	op := ic.MapAddUpd(strconv.FormatInt(int64(replicaID), 10), tools.SharedConfig.GetConfig("localPotionDBAddress"))
	ic.DoSingleUpdate(objId, op)
}

func RepIPRead() crdt.MapEntryState {
	objId := CreateKeyParams("repIPKey", proto.CRDTType_ORMAP, "admin")
	read := ic.FullRead()
	state := ic.DoSingleRead(objId, read)
	mapFullState := state.(crdt.MapEntryState)
	res := reflect.DeepEqual(mapFullState.Values, repIP)
	if !res {
		fmt.Println("[REPLICAS_IP]:", mapFullState.Values)
	}
	return mapFullState
}

func contains(s []crdt.Element, e string) bool {
	for _, a := range s {
		if string(a) == e {
			return true
		}
	}
	return false
}
