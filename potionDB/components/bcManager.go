//Keeps track of each partition's Bounded Counters (BC) and periodically asks for permissions.

//Idea: two goroutines: one that does the timer and another that processes requests

//Honestly... easier if Materializer is the one keeping track of the BCs
//Less goroutines trading messages, no problems about sharing maps, etc.

package components

import (
	fmt "fmt"
	"math/rand"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"
)

const (
	PermFreq = 1000 * time.Millisecond //ms
)

func MakeMatBCUpdPermissions(nParts int) MatBCUpdPermissions {
	return MatBCUpdPermissions{ReplyChan: make(chan MatBCUpdPermissionsReply, nParts)}
}

// Send one grouped request but "split" by partitions
func StartBCTimer(mat *Materializer, connPool *connPool, replicaIDToIndex map[int16]int, serverID int16) {
	if shared.IsBCPermSharingDisabled {
		return
	}
	nParts, replicaIDs := len(mat.channels), clocksi.GetCopyKeys()
	req := MakeMatBCUpdPermissions(nParts)
	matReq := MaterializerRequest{MatRequestArgs: req}
	bcPermsPerServer := make(map[int16][]map[crdt.KeyParams]int32) //replicaID -> [partID][keyP]->amount
	hasReqPerServer := make(map[int16]bool)
	rng := rand.NewSource(time.Now().Unix())
	go func() {
		for {
			for _, id := range replicaIDs {
				bcPermsPerServer[id] = make([]map[crdt.KeyParams]int32, nParts)
				hasReqPerServer[id] = false
			}
			time.Sleep(PermFreq)
			//fmt.Printf("[BCM]Sending request to partitions: %v+\n", req)
			mat.SendRequestToAllChannels(matReq)
			for i := 0; i < nParts; i++ {
				reply := <-req.ReplyChan
				for serverID, objPerms := range reply.ReqsMap {
					if len(objPerms) > 0 {
						bcPermsPerServer[serverID][reply.PartID] = objPerms
						hasReqPerServer[serverID] = true
					}
				}
			}
			hasAny := false
			//fmt.Println("[BCM]Processed all replies from partitions: ", hasReqPerServer)
			for id, perms := range bcPermsPerServer {
				if hasReqPerServer[id] {
					protoReq := CreateS2SWrapperProto(int32(rng.Int63()), proto.WrapperType_BC_PERMS_REQ,
						CreateBCPermissionsReqProto(perms, serverID))
					connPool.sendRequest(S2S, protoReq, replicaIDToIndex[id])
					hasAny = true
				}
			}
			if hasAny {
				fmt.Println("[BCM]Sent request to other replicas", hasReqPerServer)
			} /*else {
				fmt.Println("[BCM]BCM cycle but no request.")
			}*/

		}
		//TODO: Consider if we want to wait/receive a reply to apply the new permissions sooner.
	}()
}
