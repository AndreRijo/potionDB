package antidote

import (
	fmt "fmt"
	"math"
	"net"
	"os"
	"potionDB/src/clocksi"
	"potionDB/src/proto"
	"potionDB/src/tools"
	"strings"
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

//I think each connection only receives txns from one other replica? Will assume this.
//NOTE: Each replica still receives its own messages - it just ignores them before processing
//This might be a waste.
//TODO: I shouldn't mix Publish and Consume on the same connection, according to this: https://godoc.org/github.com/streadway/amqp

type RemoteConn struct {
	conn          *amqp.Connection
	sendCh        *amqp.Channel
	recCh         <-chan amqp.Delivery
	listenerChan  chan ReplicatorMsg
	replicaID     int16  //Replica ID of the server, not of the connection itself.
	replicaString string //Used for rabbitMQ headers in order to signal from who the msg is
	//holdOperations   map[int16]*HoldOperations
	HoldTxn
	nBucketsToListen int
	buckets          map[string]struct{}
	connID           int16 //Uniquely identifies this connection. This should not be confused with the connected replica's replicaID
	txnCount         int32 //Used to uniquely identify the transactions when sending. It does not match TxnId and overflows are OK.
	debugCount       int32
	allBuckets       bool //True if the replica's buckets has the wildcard '*' (i.e., replicates every bucket)
}

//Idea: hold operations from a replica until all operations for a partition are received.
/*type HoldOperations struct {
	lastRecUpds []*NewReplicatorRequest
	partitionID int64
	nOpsSoFar   int
	txnID       int32
}*/

type HoldTxn struct {
	onHold    []ReplicatorMsg //May receive up to 1 RemoteTxn/Group per bucket
	txnID     int32
	nReceived int
	isGroup   bool
}

/*
type BktOpsIdPair struct {
	upds  map[int][]*UpdateObjectParams //Updates per partition of a bucket of one txn
	txnID int32                         //TxnID
}
*/

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
	bucketTopicPrefix = "b." //Needed otherwise when we listen to all buckets we receive our own join messages and related.
	groupTopicPrefix  = "g."
	triggerTopic      = "trigger"
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
	allBuckets := false
	prefix := tools.SharedConfig.GetOrDefault("rabbitMQUser", "guest")
	vhost := tools.SharedConfig.GetOrDefault("rabbitVHost", "/crdts")
	prefix = prefix + ":" + prefix + "@"
	//conn, err := amqp.Dial(protocol + prefix + ip)
	link := protocol + prefix + ip + "/" + vhost
	currH, currM, currS := time.Now().Clock()
	fmt.Printf("[RC][%d:%d:%d]Connecting to %s\n", currH, currM, currS, link)
	conn, err := amqp.DialConfig(link, amqp.Config{Dial: scalateTimeout})
	if err != nil {
		currH, currM, currS = time.Now().Clock()
		fmt.Printf("[RC][%d:%d:%d]Failed to open connection to rabbitMQ at %s: %s. Retrying.\n", currH, currM, currS, ip, err)
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
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to declare join queue with rabbitMQ:", err)
		return nil, err
	}
	//The previously declared queue will receive messages from any replica that publishes updates for objects in buckets in bucketsToListen
	//bucket.* - matches anything with "bucket.(any word). * means one word, any"
	//bucket.# - matches anything with "bucket.(any amount of words). # means 0 or more words, each separated by a dot."
	for _, bucket := range bucketsToListen {
		if bucket == "*" {
			allBuckets = true
		}
		sendCh.QueueBind(replQueue.Name, bucketTopicPrefix+bucket, exchangeName, false, nil)
		//For groups
		sendCh.QueueBind(replQueue.Name, groupTopicPrefix+bucket, exchangeName, false, nil)
		//sendCh.QueueBind(replQueue.Name, "*", exchangeName, false, nil)
	}
	//We also need to associate stable clocks to the queue.
	//TODO: Some kind of filtering for this
	sendCh.QueueBind(replQueue.Name, clockTopic, exchangeName, false, nil)
	//Triggers as well
	sendCh.QueueBind(replQueue.Name, triggerTopic, exchangeName, false, nil)
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
	fmt.Printf("[RC%d]Queues names: %s, %s, %s\n", connID, replQueue.Name, joinQueue.Name, queueToListen.Name)
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
		listenerChan:  make(chan ReplicatorMsg),
		replicaID:     replicaID,
		replicaString: fmt.Sprint(replicaID),
		//holdOperations:   make(map[int16]*HoldOperations),
		HoldTxn:          HoldTxn{onHold: make([]ReplicatorMsg, len(bucketsToListen))},
		nBucketsToListen: len(bucketsToListen),
		buckets:          bucketsMap,
		connID:           connID,
		allBuckets:       allBuckets,
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
	fmt.Println("[RC][ScalateTimeout function]")
	nextTimeout := 2 * time.Second
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
			nextTimeout += (nextTimeout / 4)
			fmt.Println("[RC][ScalateTimeout function]Failed to connect to", addr, ". Retrying to connect...")
		}
	}
	return
}

//Note: This should *ONLY* be used when a declaration of one of RabbitMQ structures (exchange, queue, etc.) changes
func deleteRabbitMQStructures(ch *amqp.Channel) {
	//Also deletes queue binds
	ch.ExchangeDelete(exchangeName, false, false)
}

/*
type groupReplicateInfo struct {
	bucketOps map[string][]map[int][]*UpdateObjectParams

}*/

func (remote *RemoteConn) SendGroupTxn(txns []RemoteTxn) {
	currCount := remote.txnCount
	for i := range txns {
		txns[i].TxnID = currCount
		currCount++
	}

	//TODO: These buffers could be re-used, as the buckets and max size of a group txn is known
	bucketOps := make(map[string][]RemoteTxn)
	pos := make(map[string]*int)
	if remote.allBuckets {
		//Different logic as we don't know which buckets we have
		var currTxnBuckets map[string]map[int][]*UpdateObjectParams
		var currBucketOps []RemoteTxn
		var has bool
		for _, txn := range txns {
			currTxnBuckets = remote.txnToBuckets(txn)
			for bkt, partUpds := range currTxnBuckets {
				currBucketOps, has = bucketOps[bkt]
				if !has {
					currBucketOps = make([]RemoteTxn, 0, len(currTxnBuckets)/2) //Usually there's fewish buckets
					pos[bkt] = new(int)
				}
				bucketOps[bkt] = append(currBucketOps, RemoteTxn{SenderID: txn.SenderID, Clk: txn.Clk, TxnID: txn.TxnID, Upds: partUpds})
				*pos[bkt]++
			}
		}
	} else {
		for bkt := range remote.buckets {
			bucketOps[bkt] = make([]RemoteTxn, len(txns))
			pos[bkt] = new(int)
		}
		var currTxnBuckets map[string]map[int][]*UpdateObjectParams
		for _, txn := range txns {
			currTxnBuckets = remote.txnToBuckets(txn)
			for bkt, partUpds := range currTxnBuckets {
				bucketOps[bkt][*pos[bkt]] = RemoteTxn{SenderID: txn.SenderID, Clk: txn.Clk, TxnID: txn.TxnID, Upds: partUpds}
				*pos[bkt]++
			}
		}
	}

	for bkt, bktTxns := range bucketOps {
		if *pos[bkt] > 0 {
			protobuf := createProtoReplicateGroupTxn(remote.replicaID, txns, bktTxns[:*pos[bkt]])
			data, err := pb.Marshal(protobuf)
			if err != nil {
				fmt.Println("[RC]Error marshalling ProtoReplicateGroupTxn proto. Protobuf:", *protobuf)
				os.Exit(0)
			}
			//fmt.Printf("[RC%d]Sending group txns with IDs %d-%d for bucket %s with len %d.\n", remote.connID,
			//remote.txnCount, remote.txnCount+int32(len(txns)), bkt, len(bktTxns))
			//remote.sendCh.Publish(exchangeName, strconv.FormatInt(int64(remote.txnCount), 10)+"."+bkt, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
			remote.sendCh.Publish(exchangeName, groupTopicPrefix+bkt, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		}
	}
	remote.txnCount += int32(len(txns))
}

/*
func (remote *RemoteConn) SendGroupTxn(txns []RemoteTxn) {
	bucketOps := make(map[string][]map[int][]*UpdateObjectParams) //bucket -> txn -> part -> upds
	for bkt := range remote.buckets {
		bucketOps[bkt] = make([]map[int][]*UpdateObjectParams, len(txns))
	}
	//var currTxnBuckets map[string]BktOpsIdPair
	var currTxnBuckets map[string]map[int][]*UpdateObjectParams
	for i, txn := range txns {
		currTxnBuckets = remote.txnToBuckets(txn)
		for bkt, partUpds := range currTxnBuckets {
			bucketOps[bkt][i] = partUpds
		}
	}

	for bkt, bktTxns := range bucketOps {
		protobuf := createProtoReplicateGroupTxn(remote.replicaID, txns, bktTxns, remote.txnCount)
		data, err := pb.Marshal(protobuf)
		if err != nil {
			fmt.Println("[RC]Error marshalling ProtoReplicateGroupTxn proto. Protobuf:", *protobuf)
			os.Exit(0)
		}
		fmt.Printf("[RC%d]Sending group txns for bucket %s with len %d.\n", remote.connID, bkt, len(bktTxns))
		//remote.sendCh.Publish(exchangeName, strconv.FormatInt(int64(remote.txnCount), 10)+"."+bkt, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		remote.sendCh.Publish(exchangeName, groupTopicPrefix+bkt, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
	}
	remote.txnCount += int32(len(txns))
}
*/

func (remote *RemoteConn) SendTxn(txn RemoteTxn) {
	txn.TxnID = remote.txnCount
	//First, get the txn separated in buckets and, for each bucket, in partitions
	bucketOps := remote.txnToBuckets(txn)
	//Now, build the protos
	for bucket, upds := range bucketOps {
		protobuf := createProtoReplicateTxn(remote.replicaID, txn.Clk, upds, remote.txnCount)
		data, err := pb.Marshal(protobuf)
		if err != nil {
			remote.checkProtoError(err, protobuf, upds)
			os.Exit(0)
		}
		//fmt.Printf("[RC%d]Sending individual txn %d for bucket %s.\n", remote.connID, remote.txnCount, bucket)
		//remote.sendCh.Publish(exchangeName, strconv.FormatInt(int64(remote.txnCount), 10)+"."+bucket, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		remote.sendCh.Publish(exchangeName, bucketTopicPrefix+bucket, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
	}
	remote.txnCount++
}

func (remote *RemoteConn) txnToBuckets(txn RemoteTxn) (bucketOps map[string]map[int][]*UpdateObjectParams) {
	//We need to send one message per bucket. Note that we're sending operations out of order here - this might be relevant later on!
	//(the order for operations in the same bucket is kept however)
	//ProtoReplicateTxn: one txn; slice of partitions
	//ProtoNewRemoteTxn: one txn of a partition.
	//Re-arrange the operations into groups of buckets, with each bucket having entries for each partition
	bucketOps = make(map[string]map[int][]*UpdateObjectParams) //bucket -> part -> upds
	for partID, partUpds := range txn.Upds {
		for _, upd := range partUpds {
			entry, hasEntry := bucketOps[upd.KeyParams.Bucket]
			if !hasEntry {
				entry = make(map[int][]*UpdateObjectParams)
				bucketOps[upd.KeyParams.Bucket] = entry
			}
			partEntry, hasEntry := entry[partID]
			if !hasEntry {
				partEntry = make([]*UpdateObjectParams, 0, len(partUpds))
			}
			partEntry = append(partEntry, upd)
			entry[partID] = partEntry
		}
	}
	return
}

func (remote *RemoteConn) checkProtoError(err error, msg pb.Message, upds map[int][]*UpdateObjectParams) {
	switch typedProto := msg.(type) {
	case *proto.ProtoReplicateTxn:
		fmt.Println("[RC]Error creating ProtoReplicateTxn.")
		fmt.Println(*typedProto)
		fmt.Println(upds)
		fmt.Println("[RC]Timestamp:", (clocksi.ClockSiTimestamp{}.FromBytes(typedProto.GetTimestamp())).ToSortedString())
	}
}

/*
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
		protobuf := createProtoReplicatePart(request.SenderID, request.PartitionID, request.Timestamp, upds, remote.txnCount)
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
		fmt.Printf("[RC][%d]Sending txn at %s\n", remote.debugCount, time.Now().String())
		remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.PartitionID, 10)+"."+bucket, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Finished sending bucket ops to topic:", strconv.FormatInt(request.PartitionID, 10)+"."+bucket)
		remote.debugCount++
	}
	remote.txnCount++
}
*/

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

func (remote *RemoteConn) SendTrigger(trigger AutoUpdate, isGeneric bool) {
	//Create new gob encoder (TODO: Try making this a variable that is used ONLY for triggers)
	//Also note that this TODO won't work with new replicas for sure.

	ci := CodingInfo{}.EncInitialize()
	protobuf := CreateNewTrigger(trigger, isGeneric, ci)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of trigger request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, triggerTopic, false, false, amqp.Publishing{Body: data})
}

//This should not be called externally.
func (remote *RemoteConn) startReceiver() {
	fmt.Println("[RC] Receiver started")
	nTxnReceived := 0
	for data := range remote.recCh {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Received something!")
		//fmt.Printf("[RC%d]Receiving something (%s) at: %s\n", remote.connID, data.RoutingKey, time.Now().String())
		switch data.RoutingKey {
		case clockTopic:
			remote.handleReceivedStableClock(data.Body)
		case joinTopic:
			remote.handleReceivedJoinTopic(data.ContentType, data.Body)
		case triggerTopic:
			remote.handleReceivedTrigger(data.Body)
		default:
			if strings.HasPrefix(data.RoutingKey, groupTopicPrefix) {
				//Group
				remote.handleReceivedGroupOps(data.Body)
			} else {
				remote.handleReceivedOps(data.Body)
			}
			nTxnReceived++
		}
		//fmt.Printf("[RC%d]Finished receiving something at: %s\n", remote.connID, time.Now().String())
		//}
	}
}

func (remote *RemoteConn) handleReceivedGroupOps(data []byte) {
	protobuf := &proto.ProtoReplicateGroupTxn{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		fmt.Println("[RC][ERROR]Failed to decode bytes of received ProtoReplicateGroupTxn. Error:", err)
		os.Exit(0)
	}
	bktGroup := protoToRemoteTxnGroup(protobuf)
	//fmt.Printf("[RC%d]Received txn group %d-%d with size %d\n", remote.connID, bktGroup.MinTxnID, bktGroup.MaxTxnID, len(bktGroup.Txns))
	if bktGroup.MinTxnID != remote.txnID {
		remote.sendMerged()
		remote.createHold(bktGroup.MinTxnID)
	}
	remote.storeTxn(bktGroup, true)
}

func (remote *RemoteConn) handleReceivedOps(data []byte) {
	protobuf := &proto.ProtoReplicateTxn{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		fmt.Println("[RC][ERROR]Failed to decode bytes of received ProtoReplicateTxn. Error:", err)
		os.Exit(0)
	}
	//Each transaction must be hold until we receive all buckets
	//This is identified by receiving another transaction or a clock.
	bktTxn := protoToRemoteTxn(protobuf)
	//fmt.Printf("[RC%d]Received txn %d\n", remote.connID, bktTxn.TxnID)
	if bktTxn.TxnID != remote.txnID {
		//Need to send the previous txn that is now complete
		remote.sendMerged()
		remote.createHold(bktTxn.TxnID)
	}
	remote.storeTxn(bktTxn, false)
}

func (remote *RemoteConn) sendMerged() {
	if remote.nReceived == 0 {
		//Nothing to send. This will happen after a clock is received.
		return
	}
	if remote.isGroup {
		remote.listenerChan <- remote.getMergedTxnGroup()
	} else {
		remote.listenerChan <- remote.getMergedTxn()
	}
}

func (remote *RemoteConn) getMergedTxn() (merged *RemoteTxn) {
	merged = &RemoteTxn{}
	merged.Upds = make(map[int][]*UpdateObjectParams)
	var currReq *RemoteTxn
	//fmt.Printf("[RC%d]Merging txn. TxnID: %d; nReceived: %d, isGroup: %t. Msgs: %v\n",
	//remote.connID, remote.txnID, remote.nReceived, remote.isGroup, remote.onHold)
	for _, msg := range remote.onHold[:remote.nReceived] {
		currReq = msg.(*RemoteTxn)
		for partID, partUpds := range currReq.Upds {
			merged.Upds[partID] = append(merged.Upds[partID], partUpds...)
		}
	}
	merged.SenderID, merged.Clk = currReq.SenderID, currReq.Clk
	return
}

func (remote *RemoteConn) getMergedTxnGroup() (merged *RemoteTxnGroup) {

	merged = &RemoteTxnGroup{}
	convReqs := make([]*RemoteTxnGroup, remote.nReceived)
	pos := make([]int, remote.nReceived)
	for i, req := range remote.onHold[:remote.nReceived] {
		convReqs[i] = req.(*RemoteTxnGroup)
	}

	firstGroup := convReqs[0]
	merged.Txns, merged.SenderID = make([]RemoteTxn, 0, len(firstGroup.Txns)), firstGroup.SenderID
	merged.MinTxnID, merged.MaxTxnID = firstGroup.MinTxnID, firstGroup.MaxTxnID

	currID, maxID := firstGroup.MinTxnID, firstGroup.MaxTxnID
	currMergedUpds := make(map[int][]*UpdateObjectParams)
	var currRemoteTxn RemoteTxn
	var currClk clocksi.Timestamp
	potencialSkip := int32(math.MaxInt32) //If for all entries the nextID is bigger than currID, we can jump to the minimum on next iteration.

	//fmt.Printf("[RC%d]Merging txn group. TxnID: %d; nReceived: %d, isGroup: %t, MinID: %d, MaxID: %d, Msgs: %v\n",
	//remote.connID, remote.txnID, remote.nReceived, remote.isGroup, currID, maxID, remote.onHold[:remote.nReceived])
	for currID <= maxID {
		//fmt.Printf("[RC%d]Outer cycle. CurrID: %d\n", remote.connID, currID)
		for i, group := range convReqs {
			if pos[i] < len(group.Txns) { //If this is false, it means we already processed all txns for this bucket.
				//fmt.Printf("[RC%d]Inner cycle. Index: %d. SenderID: %d, MinTxnID: %d, MaxTxnID: %d, Len of ops: %d\n",
				//remote.connID, i, group.SenderID, group.MinTxnID, group.MaxTxnID, len(group.Txns))
				currRemoteTxn = group.Txns[pos[i]]
				if currRemoteTxn.TxnID == currID {
					//Right txn.
					for partID, partUpds := range currRemoteTxn.Upds {
						currMergedUpds[partID] = append(currMergedUpds[partID], partUpds...)
					}
					currClk = currRemoteTxn.Clk
					pos[i]++
				} else {
					//Ignore. Mark it as a potencial new minimum
					potencialSkip = tools.MinInt32(currRemoteTxn.TxnID, potencialSkip)
				}
			}
		}
		if len(currMergedUpds) > 0 {
			merged.Txns = append(merged.Txns, RemoteTxn{SenderID: merged.SenderID, Clk: currClk, Upds: currMergedUpds, TxnID: currID})
			currID++
			currMergedUpds = make(map[int][]*UpdateObjectParams)
		} else {
			//There may be no update for the txn, as the txn may only concern buckets not locally replicated.
			//In this case we found what's the next minID, can skip to it
			//As an intended side-effect, if all groups are already full processed, the cycle will terminate as currID will be maxInt32.
			currID = potencialSkip
		}
		potencialSkip = math.MaxInt32
	}
	//fmt.Printf("[RC%d]Finished txn group merge. Size: %d\n", remote.connID, len(merged.Txns))
	return
}

func (remote *RemoteConn) storeTxn(msg ReplicatorMsg, isGroup bool) {
	remote.onHold[remote.nReceived], remote.isGroup = msg, isGroup
	remote.nReceived++
}

func (remote *RemoteConn) createHold(txnID int32) {
	remote.txnID, remote.nReceived = txnID, 0
}

/*
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
		} else if holdOps.txnID == request.TxnID {
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
		fmt.Println("[RC]Received ops from self - is localRabbitMQ instance correctly configured?")
		tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Ignored request from self.")
	}
	//tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "My replicaID:", remote.replicaID, "senderID:", request.SenderID)
}
*/

func (remote *RemoteConn) handleReceivedStableClock(data []byte) {
	protobuf := &proto.ProtoStableClock{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received stableClock. Error:", err)
	}
	clkReq := protoToStableClock(protobuf)
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote stableClock:", *clkReq)
	if clkReq.SenderID == remote.replicaID {
		fmt.Println("[RC]Received clock from self - is localrabbitmq correctly configured?")
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Ignored received stableClock as it was sent by myself.")
	} else {
		/*
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
		*/
		remote.sendMerged()
		remote.createHold(0)
		remote.listenerChan <- clkReq
	}
}

func (remote *RemoteConn) handleReceivedTrigger(data []byte) {
	protobuf, ci := &proto.ApbNewTrigger{}, CodingInfo{}.DecInitialize()
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received trigger. Error:", err)
	}
	trigger := ProtoTriggerToAntidote(protobuf, ci)
	remote.listenerChan <- &RemoteTrigger{AutoUpdate: trigger, IsGeneric: protobuf.GetIsGeneric()}
}

/*
func (remote *RemoteConn) SendTrigger(trigger AutoUpdate, isGeneric bool) {
	//protobuf := CreateNewTrigger(trigger, isGeneric)
	//TODO
	//Create new gob encoder (TODO: Try making this a variable that is used ONLY for triggers)
	//Also note that this TODO won't work with new replicas for sure.

	ci := CodingInfo{}.EncInitialize()
	protobuf := CreateNewTrigger(trigger, isGeneric, ci)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of trigger request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, triggerTopic, false, false, amqp.Publishing{Body: data})
}
*/

/*
func (remote *RemoteConn) getMergedReplicatorRequest(holdOps *HoldOperations, replicaID int16) (request *NewReplicatorRequest) {
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Merging received replicator requests")
	request = &NewReplicatorRequest{
		PartitionID: holdOps.partitionID,
		SenderID:    replicaID,
		Timestamp:   holdOps.lastRecUpds[0].Timestamp,
		Upds:        make([]*UpdateObjectParams, 0, holdOps.nOpsSoFar),
		TxnID:       holdOps.txnID,
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
		txnID:       request.TxnID,
	}
	holdOps.lastRecUpds[0] = request
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Hold after creation:", *holdOps)
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Ops of request added to hold:", *holdOps.lastRecUpds[0])
	return
}
*/

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
	default:
		fmt.Printf("Unexpected join msg, ignored: %s\n", msgType)
	}
}

func (remote *RemoteConn) handleRemoteID(data []byte) {
	protobuf := &proto.ProtoRemoteID{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoRemoteID. Error:")
	senderID := int16(protobuf.GetReplicaID())
	remote.listenerChan <- &RemoteID{SenderID: senderID, Buckets: protobuf.GetMyBuckets(), IP: protobuf.GetMyIP()}
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
