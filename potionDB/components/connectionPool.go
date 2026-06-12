package components

import (
	fmt "fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"potionDB/crdt/proto"
	"potionDB/shared/shared"

	"github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

//TODO: I think when PotionDB warns TM of a new client, he can warn the connectionPool to create a new set of goroutines, up to the max

const (
	MAX_CLIENT_PER_POOL int  = 50 //Max number of clients each goroutine will handle before "blocking"
	CP_CLOSE_CONN       byte = 247
	//CP_LOST_CONN        byte = 248
	S2S_TM_CHAN_SIZE int = 20
)

var (
	MAX_POOL_PER_SERVER int64 //Max number of connections to each server
)

type connPool struct {
	//nConns      int //Per server
	nConns      atomic.Int64 //Number of connections ever opened.
	remoteIPs   []string
	alivePerIP  []atomic.Int64   //For helping with proper closing of connections.
	reqs        []chan msgToSend //Size: number of servers
	newConnChan chan bool
	wg          *sync.WaitGroup //Used to help close the connections later.
}

type msgToSend struct {
	msg       *proto.S2SWrapper
	code      byte
	replyChan chan msgReply
	needsLock bool
	//id        int //ID to identify the requester
	//lockChan  chan msgToSend
}

type msgReply struct {
	msg      *proto.S2SWrapperReply
	code     byte //type of the embedded message
	err      error
	lockChan chan msgToSend
	//id       int  //ID to identify who to reply to

}

func initializeConnPool(remoteIPs []string) (pool *connPool) {
	reqs := make([]chan msgToSend, len(remoteIPs))
	for i := range reqs {
		reqs[i] = make(chan msgToSend, tools.Max(MAX_CLIENT_PER_POOL, int(MAX_POOL_PER_SERVER)))
	}
	pool = &connPool{remoteIPs: remoteIPs, reqs: reqs, newConnChan: make(chan bool, 1000), wg: &sync.WaitGroup{}}
	pool.alivePerIP = make([]atomic.Int64, len(remoteIPs))
	go pool.newConnReceiver()
	return
}

// TODO: This can work both with ip list from rabbitMQ or tc. If from tc, make sure to filter out our own replica (look at the field tcMyPos)
// After we get to know all replicaIDs, we need to finish initialization with proper remoteIPs.
// Partially initializes connPool early, in order to attempt sharing the replicaIDs before RabbitMQ is ready, thus enabling fast PotionDB startup.
func optimisticInitializeConnPool(remoteIPs []string) (pool *connPool) {
	pool = &connPool{reqs: make([]chan msgToSend, len(remoteIPs)), newConnChan: make(chan bool, 1000), wg: &sync.WaitGroup{}}
	pool.alivePerIP = make([]atomic.Int64, len(remoteIPs))
	pool.nConns.Add(1)
	for i, ip := range remoteIPs {
		pool.reqs[i] = make(chan msgToSend, tools.Max(MAX_CLIENT_PER_POOL, int(MAX_POOL_PER_SERVER)))
		portIndex := strings.LastIndex(ip, ":") + 1
		if portIndex == 0 {
			portIndex = len(ip)
		}
		go pool.handleRequests(pool.reqs[i], ip[:portIndex]+strconv.Itoa(shared.PotionDBPort*4%65535), true, i)
	}
	return
}

// Called after we already know the replicaIDs and, thus, we are sure of the IP addresses.
func (pool *connPool) finishOptimisticInitializationConnPool(realRemoteIPs []string) {
	pool.remoteIPs = realRemoteIPs
	go pool.newConnReceiver()
}

func (pool *connPool) newConn() {
	if pool.nConns.Load() < MAX_POOL_PER_SERVER {
		//nConns value may be outdated due to concurrency, but that's okay as the other goroutine ignores requests after the max is achieved.
		//fmt.Println("[CP]Requested for new listener goroutines", pool.nConns)
		pool.newConnChan <- true
	}
}

func (pool *connPool) newConnReceiver() {
	for pool.nConns.Load() < MAX_POOL_PER_SERVER {
		<-pool.newConnChan
		pool.nConns.Add(1)
		pool.wg.Add(len(pool.remoteIPs))
		for i, ip := range pool.remoteIPs {
			go pool.handleRequests(pool.reqs[i], ip, false, i)
		}
		//pool.nConns++
		//fmt.Println("[CP]Created new listener goroutines")
	}
	for {
		<-pool.newConnChan
		//fmt.Println("[CP]Ignoring new receiver requests as there's too many already.", pool.nConns.Load(), MAX_POOL_PER_SERVER)
	}
}

func (pool *connPool) closeConnections() {
	msg := msgToSend{code: CP_CLOSE_CONN}
	leftPerServer := make([]int64, len(pool.remoteIPs))
	totalConns := 0
	for i := range pool.remoteIPs {
		nAlive := pool.alivePerIP[i].Load()
		leftPerServer[i] = nAlive
		totalConns += int(nAlive)
	}
	/*for i, nClose := range leftPerServer {
		channel := pool.reqs[i]
		for j := int64(0); j < nClose; j++ {
			channel <- msg
		}
	}*/
	/*for i := 0; i < nConns; i++ {
		for _, reqChan := range pool.reqs {
			reqChan <- msg
		}
	}*/
	fmt.Printf("[CP]Sending requests to close %d (%v) connections at %s.\n", totalConns, leftPerServer, time.Now().String())
	nServers := len(pool.remoteIPs)
	retriesPer := make([]int, nServers)
	//This version is not fast but it is correct, as it handles properly channels being full.
	for i := 0; i < totalConns; i++ {
		for j := 0; j < nServers; j++ {
			if leftPerServer[j] > 0 {
				if len(pool.reqs[j]) < cap(pool.reqs[j]) {
					pool.reqs[j] <- msg
				} else {
					retriesPer[j]++
					if retriesPer[j] == 10 {
						fmt.Printf("[CP]Too many retries on requesting to close connections for server %d. Channel keeps reporting full. Connections left for this server: %d. Channel len, cap: %d, %d.\n", j, leftPerServer[j], len(pool.reqs[j]), cap(pool.reqs[j]))
						totalConns -= int(leftPerServer[j])
						leftPerServer[j] = 0
					}
				}
			}
		}
	}
	fmt.Printf("[CP]Waiting for all connections (%d) to close at %s.\n", totalConns, time.Now().String())
	//Prepare a routine to wait on pool.wg.Wait(), in case some concurrency artifact makes us wait forever.
	done := make(chan struct{})
	go func() {
		pool.wg.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
		fmt.Printf("[CP]All connections successfully closed at %s.\n", time.Now().String())
	case <-time.After(300 * time.Millisecond):
		nLeft := pool.nConns.Load()
		if nLeft == 0 {
			fmt.Printf("[CP]Connection closing timeout fired, but it seems like all connections have been closed (nConns is 0), at %s.\n", time.Now().String())
		} else { //Last chance, if this fails then we just force exit.
			waitTime := min(500, int((float64(nLeft) * 0.9)))
			<-time.After(time.Duration(waitTime) * time.Millisecond)
			nLeft = pool.nConns.Load()
			if nLeft == 0 {
				fmt.Printf("[CP]Connection closing timeout (2nd) fired, but it seems like all connections have been closed (nConns is 0), at %s.\n", time.Now().String())
			} else {
				fmt.Printf("[CP]Connection closing timeout (2nd) fired, but it is unknown if all connections are closed (possibly left: %d), at %s. Will no longer wait.\n", nLeft, time.Now().String())
			}
		}
	}
	/*if nCloses <= 50 { //Just sleep for a bit and then return.
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("[CP]All connections closed at %s (did not wait, just slept and returned.).\n", time.Now().String())
		return
	}
	toWait := int(float64(nCloses) * 0.9)
	for i := 0; i < toWait; i++ {
		<-replyChan
		if (i % 50) == 0 {
			fmt.Printf("[CP]Closed %d connections at %s.\n", i, time.Now().String())
		}
	}*/
	//fmt.Printf("[CP]All connections closed at %s.\n", time.Now().String())
}

func (pool *connPool) sendRequest(code byte, msg *proto.S2SWrapper, serverIndex int) (replyChan chan msgReply) {
	/*debugChan := make(chan struct{}, 1)
	go func(dChan chan struct{}) { //TODO: REMOVE!
		select {
		case <-dChan:
			return
		case <-time.After(10 * time.Second):
			fmt.Printf("[CP][sendRequest]Warning: Stuck on sending request to serverIndex %d. Code: %d. ClientID: %d. MsgID: %d. Channel len, cap: %d, %d.\n",
				serverIndex, code, msg.GetClientID(), msg.GetMsgID(), len(pool.reqs[serverIndex]), cap(pool.reqs[serverIndex]))
			time.Sleep(1000 * time.Millisecond)
			os.Exit(1)
			return
		}
	}(debugChan)*/
	//Buffer prevents handleRequests from blocking
	replyChan = make(chan msgReply, 1)
	pool.reqs[serverIndex] <- msgToSend{code: code, msg: msg, replyChan: replyChan, needsLock: false}
	//debugChan <- struct{}{}
	return
}

func (pool *connPool) sendAndLockRequest(code byte, msg *proto.S2SWrapper, serverIndex int) (replyChan chan msgReply) {
	replyChan = make(chan msgReply, 1)
	pool.reqs[serverIndex] <- msgToSend{code: code, msg: msg, replyChan: replyChan, needsLock: true}
	//The goroutine that handles the request will add his own locked chan on reply
	return
}

// In this case we already know the target server is up, running and with correct IP, so we will force a connection.
func (pool *connPool) establishNormalConnection(ip string) net.Conn {
	dialer := net.Dialer{KeepAlive: -1}
	success, timeout := false, 10*time.Millisecond
	var conn net.Conn
	var err error
	nTries := 0
	for !success {
		conn, err = dialer.Dial("tcp", ip)
		if err == nil {
			success = true
		} else {
			if nTries%10 == 9 {
				fmt.Printf("[CP]Network connection establishment err on connectionPool.establishNormalConnection for ip %s. Error: %s.\n"+
					"Re-attempting connection to %s in a short while. This is likely caused by internal data loading and thus is normal.\n", ip, err, ip)
			}
			time.Sleep(timeout)
			timeout *= 2
		}
		if timeout > 2000*time.Millisecond { //Something wrong with the other server (e.g., crashed while we tried to connect). State is unknown so better exit.
			fmt.Printf("[CP][FATAL]Network connection establishment on connectionPool.establishNormalConnection for ip %s failed after several attempts. Exiting PotionDB.\n", ip)
			os.Exit(1)
		}
	}
	return conn
}

// In this case we are unsure if the server's ip (or port) is correct. We will try to connect it to attempt a faster PotionDB startup.
// If after some time the connection is still unsuccesful, we will abort and let PotionDB initialize normally through RabbitMQ replicaID sharing.
// (Actually as of now this keeps trying forever...)
func (pool *connPool) establishOptimisticConnection(ip string) (net.Conn, error) {
	dialer := net.Dialer{KeepAlive: -1}
	success, timeout := false, 50*time.Millisecond
	nAttempts := 0
	var conn net.Conn
	var err error
	for !success {
		conn, err = dialer.Dial("tcp", ip)
		if err == nil {
			if nAttempts >= 10 {
				fmt.Printf("[CP]Successful optimistic connection after %d attempts. IP: %s\n", nAttempts+1, ip)
			}
			//fmt.Println("[CP]Successful optimistic connection.")
			success = true
		} else {
			//fmt.Printf("[CP]Network connection establishment err on connectionPool.establishOptimisticConnection for ip %s. Error: %s.\n"+
			//"Re-attempting connection to %s in a short while. This is likely caused by internal data loading and thus is normal.\n", ip, err, ip)
			time.Sleep(timeout)
			timeout = time.Duration(float64(timeout) * 1.5)
			nAttempts++
		}
		if nAttempts%10 == 0 && nAttempts > 0 {
			fmt.Printf("[CP]Unsuccessful optimistic connection after %d attempts. Maybe the other server is not initialized yet or on a different machine from RabbitMQ/different server from this server? Target IP: %s. Will keep trying.\n", nAttempts, ip)
		}
	}
	return conn, err
}

// Note: It is assumed that all entities (i.e., TM) who sent requests here will block waiting for a reply.
func (pool *connPool) handleRequests(reqChan chan msgToSend, ip string, firstConn bool, serverI int) {
	var conn net.Conn
	var err error

	pool.alivePerIP[serverI].Add(1)

	//Connect to other PotionDB and send initial msg to signal that this is a server-server connection
	if firstConn {
		//fmt.Printf("[CP]Starting handler with firstConn set to true. Will try to connect and send first request. (IP: %s)\n", ip)
		conn, err = pool.establishOptimisticConnection(ip)
		if err != nil {
			return
		}
		fmt.Printf("[CP]First connection, sending server conn proto with ReplicaID at %s.\n", time.Now().Format("15:04:03.000"))
		SendProto(ServerConnReplicaID, CreateServerConnReplicaID(shared.ReplicaID, shared.Buckets, localPotionIP), conn)
		pool.alivePerIP[serverI].Add(-1)
		time.Sleep(1000 * time.Millisecond)
		conn.Close()
		return
		//We are forced to close this connection, as the IP received here is from the configs. It may be different than the one we get after we know replicaIDs.
		//Worse, the servers may be with a different order, which would mean we would be receiving here requests intended for another server.
	} else {
		conn = pool.establishNormalConnection(ip) //This one always works.
		SendProto(ServerConn, CreateServerConn(shared.ReplicaID), conn)
	}

	replyChan := make(chan msgReply, MAX_CLIENT_PER_POOL)
	lockedChan := make(chan msgToSend, MAX_CLIENT_PER_POOL) //For locked connections
	nLocked := 0

	go pool.receiver(conn, replyChan, reqChan)
	clientMap := make(map[uint64]chan msgReply)
	//clientMapTs := make(map[uint64]int64) //TODO: Remove. This is only for debugging purposes.

	/*debugChan := make(chan int, 2) //TODO: REMOVE.
	go func(dChan chan int) {
		var code int
		var replyCode int
		for {
			code = <-dChan
			switch code {
			case 1, 2, 3, 4, 5:
				//Wait for finish code, 100.
				select {
				case replyCode = <-dChan:
					if replyCode != 100 {
						fmt.Printf("[CP_DEBUG][handleRequests]Unexpected behavior: got two requests in a row without a reply inbetween. Code, 2nd code: %d, %d.\n", code, replyCode)
					}
					//Ignore.
				case <-time.After(8 * time.Second):
					fmt.Printf("[CP_DEBUG][handleRequests]Stuck handleRequests!!! A request was sent but it hasn't finished in less than 10s. Last code sent: %d.\n", code)
					time.Sleep(1200 * time.Millisecond)
					os.Exit(0)
				}
			default:
				fmt.Printf("[CP_DEBUG][handleRequests]Unexpected code received on debug channel: %d.\n", code)
				panic(0)
			}
		}
	}(debugChan)*/

	//count := 0 //TODO: Remove this count.
	//var startID, endID uint64 //Checking for ID corruption...
	//idsReq := tools.NewSliceWithCounter[uint64](1000)
	//idsReply := tools.NewSliceWithCounter[uint64](1000)

	/*go func() {
		lastMin := 0
		lastPrint := true
		for {
			time.Sleep(2 * time.Second)
			if lastMin < idsReq.Len() && lastMin < idsReply.Len() {
				lastPrint = true
				reqIDs := idsReq.ToSlice()
				replyIDs := idsReply.ToSlice()
				newMin := tools.Min(len(reqIDs), len(replyIDs))
				fmt.Printf("[CP_DEBUG]Sent client IDs: %v\nReplied client IDs: %v\n", reqIDs[lastMin:newMin], replyIDs[lastMin:newMin])
				lastMin = newMin
			} else if idsReq.Len() > idsReply.Len() && lastMin < idsReq.Len() {
				if lastPrint { //Don't print yet, wait another round.
					lastPrint = false
				} else {
					reqIDs := idsReq.ToSlice()
					fmt.Printf("[CP_DEBUG]WARNING! More req IDs than reply IDs! Req IDs: %v\n", reqIDs[lastMin:])
					lastMin = len(reqIDs)
				}
			}
		}
	}()*/

	writeBuf := make([]byte, 1000)
	for {
		/*if count%1000 == 0 {
			currTS := time.Now().UnixNano()
			var sb strings.Builder
			isAnyLate := false
			sb.WriteString(fmt.Sprintf("[CP_DEBUG, IP: %s]Possibly stuck clients: ", ip))
			for clientID, startTs := range clientMapTs {
				diffTime := currTS - startTs
				if diffTime > int64(3000*time.Millisecond) {
					isAnyLate = true
					sb.WriteString(fmt.Sprintf("%d (%dms), ", clientID, diffTime/int64(time.Millisecond)))
				}
			}
			if isAnyLate {
				fmt.Println(sb.String())
			}
		}
		count++*/
		if nLocked < MAX_CLIENT_PER_POOL {
			select {
			case reply := <-replyChan:
				//startID = reply.msg.GetClientID()
				//debugChan <- 1
				reply.lockChan = lockedChan
				clientID := reply.msg.GetClientID()
				/*fmt.Printf("[CP_DEBUG]Replying to TM with id %d.\n", uint64(clientID))
				if _, has := clientMap[clientID]; !has { //TODO: REMOVE.
					panic(fmt.Sprintf("[CP_DEBUG]Error: received reply for clientID %d, but reply chan for that client no longer exists.\n", clientID))
				}*/
				tmChan := clientMap[clientID]
				tmChan <- reply
				if pool.lastClientReq(reply.msg.GetMsgID()) {
					delete(clientMap, clientID)
					close(tmChan)
				} /* else {
					panic("[CP_DEBUG]Didn't delete map")
				}*/
				/*diffTime := time.Now().UnixNano() - clientMapTs[clientID]
				if diffTime/int64(time.Millisecond) > 3000 {
					fmt.Printf("[CP_DEBUG]Replied to TM with id %d, took %d ms.\n", clientID, diffTime/int64(time.Millisecond))
				}*/
				//delete(clientMapTs, clientID)
				//endID = reply.msg.GetClientID()
				//idsReply.Append(clientID)
				/*if startID != endID {
					panic("[CP_DEBUG]ClientID corruption during reply!!! StartID: " + strconv.FormatUint(startID, 10) + ", EndID: " + strconv.FormatUint(endID, 10))
				}*/
			case lockedReq := <-lockedChan:
				//debugChan <- 2
				if !lockedReq.needsLock {
					//Unlock
					//fmt.Println("[CP]Unlocking id", lockedReq.msg.GetClientID())
					nLocked--
				}
				//fmt.Printf("[CP]Sending proto (locked): %+v\n", lockedReq.msg)
				//start := time.Now().UnixNano()
				//SendProto(lockedReq.code, lockedReq.msg, conn)
				err, writeBuf = SendProtoReusableBufVT(lockedReq.code, lockedReq.msg, conn, writeBuf)
				//end := time.Now().UnixNano()
				//fmt.Printf("[CP][LockedReq][LockOK]Took %d ms to send proto at %v\n", (end-start)/1000000, time.Now().String())
			case req := <-reqChan:
				//startID = req.msg.GetClientID()
				//debugChan <- 3
				if req.code == CP_CLOSE_CONN {
					//Note: If in the future we use this besides for shutting down PotionDB, we may want to drain any requests in lockedChan and replyChan and give an error message of some kind.
					//And the routine responsible for sending the CP_CLOSE_CONN should drain reqChan after all connections are closed.
					//fmt.Printf("[CP_DEBUG]Got a CP_CLOSE_CONN request, closing S2S connection.\n") //TODO: Remove this print.
					conn.Close()
					//req.replyChan <- msgReply{}
					//debugChan <- 100
					pool.alivePerIP[serverI].Add(-1)
					pool.wg.Done()
					return
				} /*else if req.code == CP_LOST_CONN {
					conn.Close() //Don't do pool.wg.Done(), as the pool isn't ready.
					return
				}*/
				//fmt.Printf("[CP]Sending proto (unlocked): %+v\n", req.msg)
				//start := time.Now().UnixNano()
				/*if _, has := clientMap[req.msg.GetClientID()]; has { //TODO: REMOVE.
					panic(fmt.Sprintf("[CP_DEBUG]Error: received request for clientID %d, but reply chan for that client already exists.\n", req.msg.GetClientID()))
				}*/
				clientMap[req.msg.GetClientID()] = req.replyChan
				//SendProto(req.code, req.msg, conn)
				//SendProtoS2SDebug(req.code, req.msg, conn)
				err, writeBuf = SendProtoReusableBufVT(req.code, req.msg, conn, writeBuf)
				//idsReq.Append(req.msg.GetClientID())
				//fmt.Printf("[CP]Sent proto to ip %s, clientID %d, type %d. Bucket of first upd: %s\n",
				//	ip, req.msg.GetClientID(), req.msg.GetMsgID(), string(req.msg.GetStaticUpd().GetUpdates()[0].GetBoundobject().GetBucket()))
				//end := time.Now().UnixNano()
				//fmt.Printf("[CP][ReqChan][LockOK]Took %d ms to send proto at %v\n", (end-start)/1000000, time.Now().String())
				if req.needsLock {
					nLocked++
				}
				//clientMapTs[req.msg.GetClientID()] = time.Now().UnixNano()
				//endID = req.msg.GetClientID()
				/*if startID != endID {
					panic("[CP_DEBUG]ClientID corruption during request!!! StartID: " + strconv.FormatUint(startID, 10) + ", EndID: " + strconv.FormatUint(endID, 10))
				}*/
				//fmt.Printf("[CP_DEBUG]Storing in clientMap replyChan to clientID %d.\n", uint64(req.msg.GetClientID()))
				/*case <-time.After(4 * time.Second): //TODO: Remove
				currTS := time.Now().UnixNano()
				var sb strings.Builder
				isAnyLate := false
				sb.WriteString(fmt.Sprintf("[CP_DEBUG, NO_ACTIVITY, IP: %s]Possibly stuck clients: ", ip))
				for clientID, startTs := range clientMapTs {
					diffTime := currTS - startTs
					if diffTime > int64(3000*time.Millisecond) {
						isAnyLate = true
						sb.WriteString(fmt.Sprintf("%d (%dms), ", clientID, diffTime/int64(time.Millisecond)))
					}
				}
				if isAnyLate {
					fmt.Println(sb.String())
				}
				continue //Don't send the debug thing.*/
			}
		} else {
			//Don't listen to public channel
			select {
			case reply := <-replyChan:
				//debugChan <- 4
				reply.lockChan = lockedChan
				clientID := reply.msg.GetClientID()
				tmChan := clientMap[clientID]
				tmChan <- reply
				if pool.lastClientReq(reply.msg.GetMsgID()) {
					delete(clientMap, clientID)
					close(tmChan)
				}
			case lockedReq := <-lockedChan:
				////debugChan <- 5
				if !lockedReq.needsLock {
					//Unlock
					nLocked--
				}
				//fmt.Printf("[CP]Sending proto (locked): %+v\n", lockedReq.msg)
				//start := time.Now().UnixNano()
				//SendProto(lockedReq.code, lockedReq.msg, conn)
				err, writeBuf = SendProtoReusableBufVT(lockedReq.code, lockedReq.msg, conn, writeBuf)
				//end := time.Now().UnixNano()
				//fmt.Printf("[CP][LockedReq][FullLock]Took %d ms to send proto\n", (end-start)/1000000)
			}
		}
		//debugChan <- 100
		if err != nil {
			fmt.Printf("[CP]Error during SendProtoReusableBuf. Error: %s. Closing connection\n", err)
			conn.Close()
			return
		}
	}
}

func (pool *connPool) receiver(conn net.Conn, channel chan<- msgReply, reqChan chan msgToSend) {
	inBuf := make([]byte, 1000)
	var code byte
	var reply pb.Message
	var err error
	//ignore(inBuf, cb, code, reply, err)
	for {
		//fmt.Println("[CP]Ready to receive reply protos")
		//start := time.Now().UnixNano()
		//code, reply, err := ReceiveProto(conn) //To prevent reply's pointer of being replaced
		//Can't re-use S2SReply msg, as we'll give it to the other goroutine and we may receive new replies before the other goroutine is done with it.
		code, reply, err, inBuf = ReceiveProtoReusableBufferVT(conn, inBuf)
		/*debugChan := make(chan struct{}, 1)
		go func(dChan chan struct{}) { //TODO: REMOVE!
			select {
			case <-dChan:
				return
			case <-time.After(10 * time.Second):
				convReply := reply.(*proto.S2SWrapperReply)
				fmt.Printf("[CP][receiver]Warning: Received a proto, but somehow stuck (probably on channel <- msgReply). Sender IP: %s. Code: %d. ClientID: %d. MsgID: %d\n",
					conn.RemoteAddr().String(), code, convReply.GetClientID(), convReply.GetMsgID())
				time.Sleep(1000 * time.Millisecond)
				conn.Close()
				os.Exit(0)
				return
			}
		}(debugChan)*/
		if err != nil {
			//fmt.Printf("[CP]Closing S2S connection as there was an error: %s\n", err.Error())
			conn.Close()
			//debugChan <- struct{}{}
			reqChan <- msgToSend{code: CP_CLOSE_CONN} //Inform handleRequests that connection is closed, so it can stop trying to send messages and just exit.
			return
		}
		//end := time.Now().UnixNano()
		//fmt.Printf("[CP][Receiver]Took %d ms to receive proto at %v\n", (end-start)/1000000, time.Now().String())
		//fmt.Printf("[CP]Proto received: +%v\n", reply)
		convReply := reply.(*proto.S2SWrapperReply)
		//fmt.Println("[CP]Received reply proto of clientID", convReply.GetClientID())
		channel <- msgReply{msg: convReply, code: code, err: err}
		//debugChan <- struct{}{}
	}
}

// Returns true if the request is static, commit or abort
func (pool *connPool) lastClientReq(msgType proto.WrapperType) bool {
	convMsgType := proto.WrapperType(msgType)
	//fmt.Printf("[CP_DEBUG]Checking if msgType %d is last client req. Conv type: %d. Expecting: %d\n", msgType, convMsgType, proto.WrapperType_COMMIT)
	return convMsgType == proto.WrapperType_STATIC_READ_OBJS || convMsgType == proto.WrapperType_STATIC_UPDATE ||
		convMsgType == proto.WrapperType_COMMIT || convMsgType == proto.WrapperType_BC_PERMS_REQ
}

/*

func (pool *connPool) handleRequests(reqChan <-chan msgToSend, ip string) {
	conn, err := net.Dial("tcp", ip)
	tools.CheckErr("Network connection establishment err on connectionPool.handleRequests for ip "+ip, err)
	//var replyType byte
	//var reply pb.Message

	//Send initial msg to signal the other PotionDB that this is a server-server connection
	SendProto(ServerConn, CreateServerConn(), conn)

	for req := range reqChan {
		//fmt.Println("[CP]Handling request on ip", ip)
		if req.msg == nil {
			fmt.Println("[CP]WARNING - Sending nil request to ip", ip)
		}
		SendProto(req.code, req.msg, conn)
		replyType, reply, err := ReceiveProto(conn) //To prevent reply's pointer of being replaced
		if err != nil {
			date := time.Now().String()
			fmt.Printf("[CP]Error returned on ReceiveProto: %s. Code: %v. Reply: %v. IP: %s\n. Freezing goroutine at timestamp %s\n",
				err, replyType, reply, ip, date)
			time.Sleep(2 * time.Minute)
		}
		req.replyChan <- msgReply{msg: reply, code: replyType, err: err}
		//fmt.Println("[CP]Finished handling request on ip", ip)
		if req.needsLock {
			pool.handleLockedRequests(conn, req.lockChan)
		}
	}
	fmt.Println("[CP]HandleRequests closing. This shouldn't happen.")
}

//In this mode, the goroutine is only listening to requests from one client. The client must close the channel once it is no longer locked.
func (pool *connPool) handleLockedRequests(conn net.Conn, lockChan chan msgToSend) {
	//var replyType byte
	//var reply pb.Message
	//var err error
	for req := range lockChan {
		//fmt.Println("[CP]Handling locked request on conn", conn)
		SendProto(req.code, req.msg, conn)
		replyType, reply, err := ReceiveProto(conn)
		req.replyChan <- msgReply{msg: reply, code: replyType, err: err}
		//fmt.Println("[CP]Finished handling locked request on conn", conn)
	}
}

*/
