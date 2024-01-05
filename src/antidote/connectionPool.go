package antidote

import (
	"net"
	"potionDB/src/proto"
	"potionDB/src/tools"
)

//TODO: I think when PotionDB warns TM of a new client, he can warn the connectionPool to create a new set of goroutines, up to the max

const (
	MAX_CLIENT_PER_POOL int = 10 //Max number of clients each goroutine will handle before "blocking"
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
			go pool.handleRequests(pool.reqs[i], ip)
		}
		pool.nConns++
		//fmt.Println("[CP]Created new listener goroutines")
	}
	for {
		<-pool.newConnChan
		//fmt.Println("[CP]Ignoring new receiver requests as there's too many already.", pool.nConns, MAX_POOL_PER_SERVER)
	}
}

func (pool *connPool) sendRequest(code byte, msg *proto.S2SWrapper, serverIndex int) (replyChan chan msgReply) {
	//Buffer prevents handleRequests from blocking, and also allows the caller to ignore the reply if he so desires
	replyChan = make(chan msgReply, 1)
	pool.reqs[serverIndex] <- msgToSend{code: code, msg: msg, replyChan: replyChan}
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

func (pool *connPool) handleRequests(reqChan <-chan msgToSend, ip string) {
	dialer := net.Dialer{KeepAlive: -1}
	conn, err := dialer.Dial("tcp", ip)
	tools.CheckErr("Network connection establishment err on connectionPool.handleRequests for ip "+ip, err)
	//var replyType byte
	//var reply pb.Message

	//Send initial msg to signal the other PotionDB that this is a server-server connection
	SendProto(ServerConn, CreateServerConn(), conn)
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
				SendProto(lockedReq.code, lockedReq.msg, conn)
			case req := <-reqChan:
				SendProto(req.code, req.msg, conn)
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
				SendProto(lockedReq.code, lockedReq.msg, conn)
			}
		}
	}
}

func (pool *connPool) receiver(conn net.Conn, channel chan<- msgReply) {
	for {
		//fmt.Println("[CP]Ready to receive reply protos")
		code, reply, err := ReceiveProto(conn) //To prevent reply's pointer of being replaced
		convReply := reply.(*proto.S2SWrapperReply)
		//fmt.Println("[CP]Received reply proto of clientID", reply.(*proto.S2SWrapperReply).GetClientID())
		channel <- msgReply{msg: convReply, code: code, err: err}
	}
}

//Returns true if the request is static, commit or abort
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
