package components

import (
	fmt "fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"potionDB/crdt/proto"
	"potionDB/potionDB/utilities"
	"potionDB/shared/shared"
)

//TODO: I think when PotionDB warns TM of a new client, he can warn the connectionPool to create a new set of goroutines, up to the max

const (
	MAX_CLIENT_PER_POOL int  = 50 //Max number of clients each goroutine will handle before "blocking"
	CP_CLOSE_CONN       byte = 247
)

var (
	MAX_POOL_PER_SERVER int //Max number of connections to each server
)

type connPool struct {
	nConns      int //Per server
	remoteIPs   []string
	reqs        []chan msgToSend //Size: number of servers
	newConnChan chan bool
}

type msgToSend struct {
	msg *proto.S2SWrapper
	//id        int //ID to identify the requester
	code      byte
	replyChan chan msgReply
	needsLock bool
	//lockChan  chan msgToSend
}

type msgReply struct {
	msg *proto.S2SWrapperReply
	//id       int  //ID to identify who to reply to
	code     byte //type of the embedded message
	err      error
	lockChan chan msgToSend
}

func initializeConnPool(remoteIPs []string) (pool *connPool) {
	reqs := make([]chan msgToSend, len(remoteIPs))
	for i := range reqs {
		reqs[i] = make(chan msgToSend, MAX_POOL_PER_SERVER*2)
	}
	pool = &connPool{nConns: 0, remoteIPs: remoteIPs, reqs: reqs, newConnChan: make(chan bool, 1000)}
	go pool.newConnReceiver()
	return
}

// TODO: This can work both with ip list from rabbitMQ or tc. If from tc, make sure to filter out our own replica (look at the field tcMyPos)
// After we get to know all replicaIDs, we need to finish initialization with proper remoteIPs.
// Partially initializes connPool early, in order to attempt sharing the replicaIDs before RabbitMQ is ready, thus enabling fast PotionDB startup.
func optimisticInitializeConnPool(remoteIPs []string) (pool *connPool) {
	pool = &connPool{nConns: 0, reqs: make([]chan msgToSend, len(remoteIPs)), newConnChan: make(chan bool, 1000)}
	for i, ip := range remoteIPs {
		pool.reqs[i] = make(chan msgToSend, MAX_POOL_PER_SERVER*2)
		portIndex := strings.LastIndex(ip, ":") + 1
		if portIndex == 0 {
			portIndex = len(ip)
		}
		go pool.handleRequests(pool.reqs[i], ip[:portIndex]+strconv.Itoa(shared.PotionDBPort*4%65535), true)
	}
	pool.nConns++
	return
}

// Called after we already know the replicaIDs and, thus, we are sure of the IP addresses.
func (pool *connPool) finishOptimisticInitializationConnPool(realRemoteIPs []string) {
	pool.remoteIPs = realRemoteIPs
	go pool.newConnReceiver()
}

func (pool *connPool) newConn() {
	if pool.nConns < MAX_POOL_PER_SERVER {
		//nConns value may be outdated due to concurrency, but that's okay as the other goroutine ignores requests after the max is achieved.
		//fmt.Println("[CP]Requested for new listener goroutines", pool.nConns)
		pool.newConnChan <- true
	}
}

func (pool *connPool) newConnReceiver() {
	for pool.nConns < MAX_POOL_PER_SERVER {
		<-pool.newConnChan
		for i, ip := range pool.remoteIPs {
			go pool.handleRequests(pool.reqs[i], ip, false)
		}
		pool.nConns++
		//fmt.Println("[CP]Created new listener goroutines")
	}
	for {
		<-pool.newConnChan
		fmt.Println("[CP]Ignoring new receiver requests as there's too many already.", pool.nConns, MAX_POOL_PER_SERVER)
	}
}

func (pool *connPool) closeConnections() {
	nCloses := len(pool.reqs) * utilities.MinInt(MAX_POOL_PER_SERVER, pool.nConns)
	replyChan := make(chan msgReply, nCloses)
	fmt.Printf("[CP]Sending requests to close connections at %s.\n", time.Now().String())
	for i := 0; i < pool.nConns; i++ {
		for _, reqChan := range pool.reqs {
			reqChan <- msgToSend{code: CP_CLOSE_CONN, replyChan: replyChan}
		}
	}
	fmt.Printf("[CP]Waiting for all connections (%d) to close at %s.\n", nCloses, time.Now().String())
	if nCloses <= 50 { //Just sleep for a bit and then return.
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
	}
	fmt.Printf("[CP]All connections closed at %s.\n", time.Now().String())
}

func (pool *connPool) sendRequest(code byte, msg *proto.S2SWrapper, serverIndex int) (replyChan chan msgReply) {
	//Buffer prevents handleRequests from blocking, and also allows the caller to ignore the reply if he so desires
	replyChan = make(chan msgReply, 1)
	pool.reqs[serverIndex] <- msgToSend{code: code, msg: msg, replyChan: replyChan, needsLock: false}
	return
}

/*
//Locks the connection to this client besides sending the request
func (pool *connPool) sendAndLockRequest(code byte, msg pb.Message, serverIndex int) (replyChan chan msgReply, lockChan chan msgToSend) {
	replyChan, lockChan = make(chan msgReply, 1), make(chan msgToSend, 1)
	pool.reqs[serverIndex] <- msgToSend{code: code, msg: msg, replyChan: replyChan, needsLock: true, lockChan: lockChan}
	return
}
*/

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
	for !success {
		conn, err = dialer.Dial("tcp", ip)
		if err == nil {
			success = true
		} else {
			fmt.Printf("[CP]Network connection establishment err on connectionPool.establishNormalConnection for ip %s. Error: %s.\n"+
				"Re-attempting connection to %s in a short while. This is likely caused by internal data loading and thus is normal.\n", ip, err, ip)
			time.Sleep(timeout)
			timeout *= 2
		}
		if timeout > 1000*time.Millisecond { //Something wrong with the other server (e.g., crashed while we tried to connect). State is unknown so better exit.
			fmt.Printf("[CP]Network connection establishment on connectionPool.establishNormalConnection for ip %s failed after several attempts. Exiting PotionDB.\n", ip)
			os.Exit(1)
		}
	}
	return conn
}

// In this case we are unsure if the server's ip (or port) is correct. We will try to connect it to attempt a faster PotionDB startup.
// If after some time the connection is still unsuccesful, we will abort and let PotionDB initialize normally through RabbitMQ replicaID sharing.
func (pool *connPool) establishOptimisticConnection(ip string) (net.Conn, error) {
	dialer := net.Dialer{KeepAlive: -1}
	success, timeout := false, 50*time.Millisecond
	nAttempts := 0
	var conn net.Conn
	var err error
	for !success {
		conn, err = dialer.Dial("tcp", ip)
		if err == nil {
			//fmt.Println("[CP]Successful optimistic connection.")
			success = true
		} else {
			//fmt.Printf("[CP]Network connection establishment err on connectionPool.establishOptimisticConnection for ip %s. Error: %s.\n"+
			//"Re-attempting connection to %s in a short while. This is likely caused by internal data loading and thus is normal.\n", ip, err, ip)
			time.Sleep(timeout)
			timeout = time.Duration(float64(timeout) * 1.5)
			nAttempts++
		}
		if nAttempts == 10 {
			fmt.Printf("[CP]Unsuccessful optimistic connection after 10 attempts. Maybe the other server is not initialized yet or on a different machine from RabbitMQ/different server from this server? Target IP: %s\n", ip)

		}
	}
	return conn, err
}

func (pool *connPool) handleRequests(reqChan <-chan msgToSend, ip string, firstConn bool) {
	var conn net.Conn
	var err error

	//Connect to other PotionDB and send initial msg to signal that this is a server-server connection
	if firstConn {
		//fmt.Printf("[CP]Starting handler with firstConn set to true. Will try to connect and send first request. (IP: %s)\n", ip)
		conn, err = pool.establishOptimisticConnection(ip)
		if err != nil {
			return
		}
		fmt.Printf("[CP]First connection, sending server conn proto with ReplicaID at %s.\n", time.Now().Format("15:04:03.000"))
		SendProto(ServerConnReplicaID, CreateServerConnReplicaID(shared.ReplicaID, shared.Buckets, localPotionIP), conn)
		pool.nConns--
		time.Sleep(1000 * time.Millisecond)
		conn.Close()
		return
	} else {
		conn = pool.establishNormalConnection(ip) //This one always works.
		SendProto(ServerConn, CreateServerConn(), conn)
	}

	replyChan := make(chan msgReply, MAX_CLIENT_PER_POOL)
	lockedChan := make(chan msgToSend, MAX_CLIENT_PER_POOL) //For locked connections
	nLocked := 0

	go pool.receiver(conn, replyChan)
	clientMap := make(map[int32]chan msgReply)

	for {
		if nLocked < MAX_CLIENT_PER_POOL {
			select {
			case reply := <-replyChan:
				reply.lockChan = lockedChan
				clientID := reply.msg.GetClientID()
				//fmt.Println("[CP]Replying to TM with id", clientID)
				clientMap[clientID] <- reply
				if pool.lastClientReq(reply.code) {
					//fmt.Println("[CP]Deleting channel in clientMap of id", clientID)
					delete(clientMap, clientID)
				}
			case lockedReq := <-lockedChan:
				if !lockedReq.needsLock {
					//Unlock
					//fmt.Println("[CP]Unlocking id", lockedReq.msg.GetClientID())
					nLocked--
				}
				//fmt.Printf("[CP]Sending proto (locked): %+v\n", lockedReq.msg)
				//start := time.Now().UnixNano()
				SendProto(lockedReq.code, lockedReq.msg, conn)
				//end := time.Now().UnixNano()
				//fmt.Printf("[CP][LockedReq][LockOK]Took %d ms to send proto at %v\n", (end-start)/1000000, time.Now().String())
			case req := <-reqChan:
				if req.code == CP_CLOSE_CONN {
					conn.Close()
					req.replyChan <- msgReply{}
					break
				}
				//fmt.Printf("[CP]Sending proto (unlocked): %+v\n", req.msg)
				//start := time.Now().UnixNano()
				SendProto(req.code, req.msg, conn)
				//end := time.Now().UnixNano()
				//fmt.Printf("[CP][ReqChan][LockOK]Took %d ms to send proto at %v\n", (end-start)/1000000, time.Now().String())
				if req.needsLock {
					nLocked++
				}
				//fmt.Println("[CP]Made channel to TM with id", req.msg.GetClientID())
				clientMap[req.msg.GetClientID()] = req.replyChan
			}
		} else {
			//Don't listen to public channel
			select {
			case reply := <-replyChan:
				reply.lockChan = lockedChan
				clientID := reply.msg.GetClientID()
				//fmt.Println("[CP]Replying to TM with id", clientID)
				clientMap[clientID] <- reply
				if pool.lastClientReq(reply.code) {
					//fmt.Println("[CP]Deleting channel in clientMap of id", clientID)
					delete(clientMap, clientID)
				}
			case lockedReq := <-lockedChan:
				if !lockedReq.needsLock {
					//Unlock
					nLocked--
				}
				//fmt.Printf("[CP]Sending proto (locked): %+v\n", lockedReq.msg)
				//start := time.Now().UnixNano()
				SendProto(lockedReq.code, lockedReq.msg, conn)
				//end := time.Now().UnixNano()
				//fmt.Printf("[CP][LockedReq][FullLock]Took %d ms to send proto\n", (end-start)/1000000)
			}
		}
	}
}

func (pool *connPool) receiver(conn net.Conn, channel chan<- msgReply) {
	for {
		//fmt.Println("[CP]Ready to receive reply protos")
		//start := time.Now().UnixNano()
		code, reply, err := ReceiveProto(conn) //To prevent reply's pointer of being replaced
		if err != nil {
			fmt.Printf("[CP]Closing S2S connection as there was an error: %s\n", err.Error())
			conn.Close()
			return
		}
		//end := time.Now().UnixNano()
		//fmt.Printf("[CP][Receiver]Took %d ms to receive proto at %v\n", (end-start)/1000000, time.Now().String())
		//fmt.Printf("[CP]Proto received: +%v\n", reply)
		convReply := reply.(*proto.S2SWrapperReply)
		//fmt.Println("[CP]Received reply proto of clientID", reply.(*proto.S2SWrapperReply).GetClientID())
		channel <- msgReply{msg: convReply, code: code, err: err}
	}
}

// Returns true if the request is static, commit or abort
func (pool *connPool) lastClientReq(msgType byte) bool {
	convMsgType := proto.WrapperType(msgType)
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
