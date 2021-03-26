package antidote

import (
	fmt "fmt"
	"net"
	"proto"
	"strconv"
	"time"
	"tools"

	pb "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

//NOTE: Each replica still receives its own messages - it just ignores them before processing
//This might be a waste.
//TODO: I shouldn't mix Publish and Consume on the same connection, according to this: https://godoc.org/github.com/streadway/amqp

type RemoteConn struct {
	conn             *amqp.Connection
	sendCh           *amqp.Channel
	recCh            <-chan amqp.Delivery
	listenerChan     chan ReplicatorMsg
	replicaID        int16
	replicaString    string //Used for rabbitMQ headers in order to signal from who the msg is
	holdOperations   map[int16]*HoldOperations
	nBucketsToListen int
	buckets          map[string]struct{}
	connID           int16 //Uniquely identifies this connection. This should not be confused with the connected replica's replicaID
}

//Idea: hold operations from a replica until all operations for a partition are received.
type HoldOperations struct {
	lastRecUpds []*NewReplicatorRequest
	partitionID int64
	nOpsSoFar   int
}

const (
	protocol = "amqp://"
	//ip                  = "guest:guest@localhost:"
	//prefix = "guest:guest@"
	//prefix = "test:test@"
	//port         = "5672/"
	exchangeName = "objRepl"
	exchangeType = "topic"
	//Go back to using this to buffer requests if we stop using remoteGroup
	//defaultListenerSize = 100
	clockTopic        = "clk"
	joinTopic         = "join"
	remoteIDContent   = "remoteID"
	joinContent       = "join"
	replyJoinContent  = "replyJoin"
	requestBktContent = "requestBkt"
	replyBktContent   = "replyBkt"
	replyEmptyContent = "replyEmpty"
	//replQueueName     = "repl"
	joinQueueName = "join"
)

var ()

//Topics (i.e., queue filters): partitionID.bucket
//There's one queue per replica. Each replica's queue will receive messages from ALL other replicas, as long as the topics match the ones
//that were binded to the replica's queue.
//Ip includes both ip and port in format: ip:port
//In the case of the connection to the local replica's RabbitMQ server, we don't consume the self messages (isSelfConn)
func CreateRemoteConnStruct(ip string, bucketsToListen []string, replicaID int16, connID int16, isSelfConn bool) (remote *RemoteConn, err error) {
	//conn, err := amqp.Dial(protocol + prefix + ip + port)
	prefix := tools.SharedConfig.GetOrDefault("rabbitMQUser", "guest")
	vhost := tools.SharedConfig.GetOrDefault("rabbitVHost", "/")
	/*
		if prefix == "" {
			prefix = "guest"
		}
		if vhost == "" {
			vhost = "/"
		}
	*/
	prefix = prefix + ":" + prefix + "@"
	//conn, err := amqp.Dial(protocol + prefix + ip)
	link := protocol + prefix + ip + "/" + vhost
	fmt.Println("Connecting to", link)
	conn, err := amqp.DialConfig(link, amqp.Config{Dial: scalateTimeout})
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to open connection to rabbitMQ at", ip, ":", err, ". Retrying.")
		time.Sleep(4000 * time.Millisecond)
		return CreateRemoteConnStruct(ip, bucketsToListen, replicaID, connID, isSelfConn)
	}
	sendCh, err := conn.Channel()
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to obtain channel from rabbitMQ:", err)
		return nil, err
	}

	//Call this to delete existing exchange/queues/binds/etc if configurations are changed
	//deleteRabbitMQStructures(sendCh)

	//We send msgs to the exchange
	//sendCh.ExchangeDelete(exchangeName, false, true)
	err = sendCh.ExchangeDeclare(exchangeName, exchangeType, false, false, false, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to declare exchange with rabbitMQ:", err)
		return nil, err
	}
	//This queue will store messages sent from other replicas
	replQueue, err := sendCh.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to declare queue with rabbitMQ:", err)
		return nil, err
	}
	joinQueue, err := sendCh.QueueDeclare(joinQueueName, false, true, false, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to declare queue with rabbitMQ:", err)
		return nil, err
	}
	//The previously declared queue will receive messages from any replica that publishes updates for objects in buckets in bucketsToListen
	//TODO: Probably remove the partition part? (i.e., the *.)
	for _, bucket := range bucketsToListen {
		sendCh.QueueBind(replQueue.Name, "*."+bucket, exchangeName, false, nil)
		//sendCh.QueueBind(replQueue.Name, "*", exchangeName, false, nil)
	}
	//We also need to associate stable clocks to the queue.
	//TODO: Some kind of filtering for this
	sendCh.QueueBind(replQueue.Name, clockTopic, exchangeName, false, nil)
	//Associate join requests to the join queue.
	sendCh.QueueBind(joinQueue.Name, joinTopic, exchangeName, false, nil)

	//This channel is used to read from the queue.
	//If this is a connection to the self rabbitMQ instance, then we only listen to join-related msgs.
	//Otherwise, we listen exclusively to replication msgs.
	var recCh <-chan amqp.Delivery = nil
	var queueToListen amqp.Queue
	if !isSelfConn {
		queueToListen = replQueue
	} else {
		queueToListen = joinQueue
	}
	recCh, err = sendCh.Consume(queueToListen.Name, "", true, false, false, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to obtain consumer from rabbitMQ:", err)
		return nil, err
	}

	bucketsMap := make(map[string]struct{})
	for _, bkt := range bucketsToListen {
		bucketsMap[bkt] = struct{}{}
	}

	remote = &RemoteConn{
		sendCh: sendCh,
		conn:   conn,
		recCh:  recCh,
		//listenerChan:     make(chan ReplicatorMsg, defaultListenerSize),
		listenerChan:     make(chan ReplicatorMsg),
		replicaID:        replicaID,
		replicaString:    fmt.Sprint(replicaID),
		holdOperations:   make(map[int16]*HoldOperations),
		nBucketsToListen: len(bucketsToListen),
		buckets:          bucketsMap,
		connID:           connID,
	}
	if !isSelfConn {
		fmt.Println("Listening to repl on connection to", ip, "with id", remote.connID)
	} else {
		fmt.Println("Listening to join on connection to", ip, "with id", remote.connID)
	}
	go remote.startReceiver()
	return
}

func scalateTimeout(network, addr string) (conn net.Conn, err error) {
	//Also control the timeout locally due to the localhost case which returns immediatelly
	nextTimeout := 4 * time.Second
	lastAttempt := time.Now().UnixNano()
	for {
		conn, err = net.DialTimeout(network, addr, nextTimeout)
		if err == nil {
			break
		} else {
			//Controlling timeout locally
			thisAttempt := time.Now().UnixNano()
			if thisAttempt-lastAttempt < int64(nextTimeout) {
				time.Sleep(nextTimeout - time.Duration(thisAttempt-lastAttempt))
			}
			lastAttempt = thisAttempt
			nextTimeout += (nextTimeout / 2)
			fmt.Println("Failed to connect to", addr, ". Retrying to connect...")
		}
	}
	return
}

//Note: This should *ONLY* be used when a declaration of one of RabbitMQ structures (exchange, queue, etc.) changes
func deleteRabbitMQStructures(ch *amqp.Channel) {
	//Also deletes queue binds
	ch.ExchangeDelete(exchangeName, false, false)
}

func (remote *RemoteConn) SendPartTxn(request *NewReplicatorRequest) {
	tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending remote request:", *request)
	if len(request.Upds) > 0 {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually sending txns to other replicas. Sending ops:", request.Upds)
	}

	//We need to send one message per bucket. Note that we're sending operations out of order here - this might be relevant later on!
	bucketOps := make(map[string][]*UpdateObjectParams)
	for _, upd := range request.Upds {
		entry, hasEntry := bucketOps[upd.KeyParams.Bucket]
		if !hasEntry {
			entry = make([]*UpdateObjectParams, 0, len(request.Upds))
		}
		entry = append(entry, upd)
		bucketOps[upd.KeyParams.Bucket] = entry
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Entry upds:", entry)
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Map's entry upds:", bucketOps[upd.KeyParams.Bucket])
	}

	for bucket, upds := range bucketOps {
		protobuf := createProtoReplicatePart(request.SenderID, request.PartitionID, request.Timestamp, upds)
		data, err := pb.Marshal(protobuf)
		//_, err := pb.Marshal(protobuf)
		if err != nil {
			tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of partTxn request to send. Error:", err)
			fmt.Println(protobuf)
			fmt.Println(upds)
			fmt.Println("Timestamp:", request.Timestamp)
			downUpdsProtos := protobuf.GetTxn().GetUpds()
			for _, proto := range downUpdsProtos {
				fmt.Println("Upd proto:", proto)
			}
		}
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending bucket ops to topic:", strconv.FormatInt(request.PartitionID, 10)+"."+bucket)
		remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.PartitionID, 10)+"."+bucket, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Finished sending bucket ops to topic:", strconv.FormatInt(request.PartitionID, 10)+"."+bucket)
	}
}

func (remote *RemoteConn) SendStableClk(ts int64) {
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending stable clk:", ts)
	protobuf := createProtoStableClock(remote.replicaID, ts)
	data, err := pb.Marshal(protobuf)
	//_, err := pb.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of stableClk request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, clockTopic, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
}

//This should not be called externally.
func (remote *RemoteConn) startReceiver() {
	fmt.Println("[RC] Receiver started")
	for data := range remote.recCh {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Received something!")

		switch data.RoutingKey {
		case clockTopic:
			remote.handleReceivedStableClock(data.Body)
		case joinTopic:
			remote.handleReceivedJoinTopic(data.ContentType, data.Body)
		default:
			//Ops have a topic based on the partition ID + bucket
			remote.handleReceivedOps(data.Body)
		}
		//}
	}
}

func (remote *RemoteConn) handleReceivedOps(data []byte) {
	protobuf := &proto.ProtoReplicatePart{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received request. Error:", err)
	}
	request := protoToReplicatorRequest(protobuf)
	//remote.decBuf.Reset()
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote request:", *request)
	if len(request.Upds) > 0 && request.SenderID != remote.replicaID {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually received txns from other replicas.")
	}
	if request.SenderID != remote.replicaID {
		holdOps, hasHold := remote.holdOperations[request.SenderID]
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Processing received remote transactions.")
		if !hasHold {
			//Initial case
			tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Initial case - Creating hold for remote ops.")
			remote.holdOperations[request.SenderID] = remote.buildHoldOperations(request)
		} else if holdOps.partitionID == request.PartitionID {
			//Hold the received operations
			tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Adding remote ops to hold.")
			holdOps.lastRecUpds = append(holdOps.lastRecUpds, request)
			holdOps.nOpsSoFar += len(request.Upds)
		} else {
			//This request corresponds to a different partition from the one we have cached. As such, we can send the cached partition's updates
			tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Different partitions - sending previous ops to replicator and creating hold for the ones received now.")
			reqToSend := remote.getMergedReplicatorRequest(holdOps, request.SenderID)
			remote.holdOperations[request.SenderID] = remote.buildHoldOperations(request)
			remote.listenerChan <- reqToSend
		}
	} else {
		fmt.Println("NOT SUPPOSED TO HAPPEN NOW!")
		tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Ignored request from self.")
	}
	//tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "My replicaID:", remote.replicaID, "senderID:", request.SenderID)
}

func (remote *RemoteConn) handleReceivedStableClock(data []byte) {
	protobuf := &proto.ProtoStableClock{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received stableClock. Error:", err)
	}
	clkReq := protoToStableClock(protobuf)
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote stableClock:", *clkReq)
	if clkReq.SenderID == remote.replicaID {
		fmt.Println("ALSO NOT SUPPOSED TO HAPPEN NOW!")
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Ignored received stableClock as it was sent by myself.")
	} else {
		//We also need to send a request with the last partition ops, if there's any
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Processing received stableClock.")
		holdOperations, hasOps := remote.holdOperations[clkReq.SenderID]
		if hasOps {
			tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Merging previous requests before sending stableClock to replicator.")
			partReq := remote.getMergedReplicatorRequest(holdOperations, clkReq.SenderID)
			remote.listenerChan <- partReq
		} else {
			tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "No previous request.")
		}
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending stableClock to replicator.")
		remote.listenerChan <- clkReq
	}
}

func (remote *RemoteConn) getMergedReplicatorRequest(holdOps *HoldOperations, replicaID int16) (request *NewReplicatorRequest) {
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Merging received replicator requests")
	request = &NewReplicatorRequest{
		PartitionID: holdOps.partitionID,
		SenderID:    replicaID,
		Timestamp:   holdOps.lastRecUpds[0].Timestamp,
		Upds:        make([]*UpdateObjectParams, 0, holdOps.nOpsSoFar),
	}
	for _, reqs := range holdOps.lastRecUpds {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Merging upds:", reqs.Upds)
		request.Upds = append(request.Upds, reqs.Upds...)
	}
	delete(remote.holdOperations, replicaID)
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Upds in merged request:", request.Upds)
	return
}

func (remote *RemoteConn) buildHoldOperations(request *NewReplicatorRequest) (holdOps *HoldOperations) {
	holdOps = &HoldOperations{
		lastRecUpds: make([]*NewReplicatorRequest, 1, remote.nBucketsToListen),
		partitionID: request.PartitionID,
		nOpsSoFar:   len(request.Upds),
	}
	holdOps.lastRecUpds[0] = request
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Hold after creation:", *holdOps)
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Ops of request added to hold:", *holdOps.lastRecUpds[0])
	return
}

func (remote *RemoteConn) GetNextRemoteRequest() (request ReplicatorMsg) {
	//Wait until all operations for a partition arrive. We can detect this in two ways:
	//1st - the next operation we receive is for a different partition
	//2nd - the next operation is a clock update
	return <-remote.listenerChan
}

/***** JOINING LOGIC *****/

func (remote *RemoteConn) SendRemoteID(idData []byte) {
	remote.sendCh.Publish(exchangeName, joinTopic, false, false,
		amqp.Publishing{CorrelationId: remote.replicaString, ContentType: remoteIDContent, Body: idData})
}

func (remote *RemoteConn) SendJoin(joinData []byte) {
	fmt.Println("[RC]Sending join as", remote.replicaID, remote.connID)
	remote.sendCh.Publish(exchangeName, joinTopic, false, false,
		amqp.Publishing{CorrelationId: remote.replicaString, ContentType: joinContent, Body: joinData})
}

func (remote *RemoteConn) SendReplyJoin(req ReplyJoin) {
	protobuf := createProtoReplyJoin(req)
	data := remote.encodeProtobuf(protobuf, "Failed to generate bytes of ReplyJoin msg.")
	remote.sendCh.Publish(exchangeName, joinTopic, false, false,
		amqp.Publishing{CorrelationId: remote.replicaString, ContentType: replyJoinContent, Body: data})
}

func (remote *RemoteConn) SendRequestBucket(req RequestBucket) {
	protobuf := createProtoRequestBucket(req)
	data := remote.encodeProtobuf(protobuf, "Failed to generate bytes of RequestBucket msg.")
	remote.sendCh.Publish(exchangeName, joinTopic, false, false,
		amqp.Publishing{CorrelationId: remote.replicaString, ContentType: requestBktContent, Body: data})
}

func (remote *RemoteConn) SendReplyBucket(req ReplyBucket) {
	protobuf := createProtoReplyBucket(req)
	data := remote.encodeProtobuf(protobuf, "Failed to generate bytes of ReplyBucket msg.")
	remote.sendCh.Publish(exchangeName, joinTopic, false, false,
		amqp.Publishing{CorrelationId: remote.replicaString, ContentType: replyBktContent, Body: data})
}

func (remote *RemoteConn) SendReplyEmpty() {
	protobuf := createProtoReplyEmpty()
	data := remote.encodeProtobuf(protobuf, "Failed to generate bytes of ReplyEmpty msg.")
	remote.sendCh.Publish(exchangeName, joinTopic, false, false,
		amqp.Publishing{CorrelationId: remote.replicaString, ContentType: replyEmptyContent, Body: data})
}

func (remote *RemoteConn) handleReceivedJoinTopic(msgType string, data []byte) {
	switch msgType {
	case remoteIDContent:
		fmt.Println("Join msg is a remoteID")
		remote.handleRemoteID(data)
	case joinContent:
		fmt.Println("Join msg is a join")
		remote.handleJoin(data)
	case replyJoinContent:
		fmt.Println("Join msg is a replyJoin")
		remote.handleReplyJoin(data)
	case requestBktContent:
		fmt.Println("Join msg is a requestBkt")
		remote.handleRequestBkt(data)
	case replyBktContent:
		fmt.Println("Join msg is a replyBkt")
		remote.handleReplyBkt(data)
	case replyEmptyContent:
		fmt.Println("Join msg is a replyEmpty")
		remote.handleReplyEmpty(data)
	}
}

func (remote *RemoteConn) handleRemoteID(data []byte) {
	protobuf := &proto.ProtoRemoteID{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoRemoteID. Error:")
	remote.listenerChan <- &RemoteID{SenderID: int16(protobuf.GetReplicaID())}
}

func (remote *RemoteConn) handleJoin(data []byte) {
	protobuf := &proto.ProtoJoin{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoJoin. Error:")
	otherBkts, senderID, ip := protoToJoin(protobuf)
	fmt.Println("Join from:", senderID, remote.connID)

	commonBkts := make([]string, len(otherBkts))
	nCommon := 0
	for _, bkt := range otherBkts {
		if _, has := remote.buckets[bkt]; has {
			commonBkts[nCommon] = bkt
			nCommon++
		}
	}
	commonBkts = commonBkts[:nCommon]
	if len(commonBkts) > 0 {
		//fmt.Println("RemoteConnID:", remote.connID)
		remote.listenerChan <- &Join{SenderID: int16(senderID), ReplyID: remote.connID, CommonBkts: commonBkts, ReqIP: ip}
	} else {
		//TODO: Ask group to send empty
	}
}

func (remote *RemoteConn) handleReplyJoin(data []byte) {
	protobuf := &proto.ProtoReplyJoin{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoReplyJoin. Error:")
	buckets, clks, senderID := protoToReplyJoin(protobuf)
	fmt.Println("ReplyJoin from:", senderID, remote.connID, remote.sendCh)
	remote.listenerChan <- &ReplyJoin{SenderID: senderID, ReplyID: remote.connID, Clks: clks, CommonBkts: buckets, ReqIP: protobuf.GetReplicaIP()}
}

func (remote *RemoteConn) handleRequestBkt(data []byte) {
	protobuf := &proto.ProtoRequestBucket{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoRequestBucket. Error:")
	buckets, senderID := protoToRequestBucket(protobuf)
	fmt.Println("RequestBkt from:", senderID, remote.connID)
	remote.listenerChan <- &RequestBucket{SenderID: senderID, ReplyID: remote.connID, Buckets: buckets, ReqIP: protobuf.GetReplicaIP()}
}

func (remote *RemoteConn) handleReplyBkt(data []byte) {
	protobuf := &proto.ProtoReplyBucket{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoReplyBucket. Error:")
	states, clk, senderID := protoToReplyBucket(protobuf)
	fmt.Println("ReplyBkt from:", senderID, remote.connID)
	remote.listenerChan <- &ReplyBucket{SenderID: senderID, PartStates: states, Clk: clk}
}

func (remote *RemoteConn) handleReplyEmpty(data []byte) {
	remote.listenerChan <- &ReplyEmpty{}
}

func (remote *RemoteConn) decodeProtobuf(protobuf pb.Message, data []byte, errMsg string) {
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, errMsg, err)
	}
}

func (remote *RemoteConn) encodeProtobuf(protobuf pb.Message, errMsg string) (data []byte) {
	data, err := pb.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, errMsg, err)
	}
	return
}
