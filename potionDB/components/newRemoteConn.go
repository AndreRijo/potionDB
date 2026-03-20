package components

import (
	fmt "fmt"
	"math/rand"
	"net"
	"os"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"potionDB/potionDB/utilities"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/AndreRijo/go-tools/src/tools"
	"github.com/streadway/amqp"
	pb "google.golang.org/protobuf/proto"
)

type RemoteConn struct {
	conn            *amqp.Connection
	sendCh          *amqp.Channel
	recCh           <-chan amqp.Delivery
	listenerChan    chan ReplicatorMsg //Channel to forward the received txns/msgs to Replicator
	replicaID       uint16             //Replica ID of the server, not of the connection itself.
	connID          uint16             //Uniquely identifies this connection. This should not be confused with the connected replica's replicaID
	allBuckets      bool               //True if the replica's buckets has the wildcard '*' (i.e., replicates every bucket)
	connEstablished bool
	replicaString   string //Used for rabbitMQ headers in order to signal from who the msg is
	HoldTxn
	nBucketsToListen int32
	buckets          map[string]struct{}
	replCount        int32 //Used to uniquely identify requests when sending. It does not match TxnId and overflows are OK.
	debugCount       int32
	workChan         chan RCWork //Channel to send marshalling requests.

	//New vars added
	replReqChan     chan RCWork //Channel to receive replication requests from the Replicator.
	senderRoutineCh chan RCSendRequest
	replDebugData   //TODO: Comment
}

type replDebugData struct {
	debugChan chan any
}

type StatisticsMarshall struct {
	protoCreationTime int64
	marshallTime      int64
}

type StatisticsTxnPrep struct {
	splitTime        int64
	toProtoTotalTime int64 //protoCreationTime + marshallTime of the whole txn.
}

type StatisticsLargeMsgRec struct { //Unused for now.
	copyTime int64
}

type StatisticsTxnRec struct {
	unmarshallTime int64
	convertTime    int64
}

type StatisticsTxnRecComplete struct {
	mergeTime int64
}

// Holds received data regarding a single txn.
type HoldTxn struct {
	onHold        tools.SliceWithCounter[RemoteTxn] //Each entry corresponds to the operations of a single txn, regarding its multiple buckets.
	txnID         int32
	partBuf       []tools.SliceWithCounter[crdt.UpdateObjectParams]
	partsInvolved tools.BitSet
}

type PrepareTxnWork struct {
	txn   RemoteTxn
	reqId int32
}

type PairKeyBytes struct {
	Key  string
	Data []byte
}

type RCSendRequest interface {
	GetMsgType() RC_REQ_TYPE //For signature purposes mostly.
	GetID() int32
}

type RCTxnReq struct {
	bktTxn tools.SliceMap[string, []byte] //Bucket, data (proto bytes)
	reqId  int32
}

type RCClkReq struct {
	data  []byte
	reqId int32
}
type RCIdReq struct{ data []byte }
type RCJoinReq struct{ data []byte }
type RCReplyJoinReq struct{ data []byte }
type RCReplyEmptyReq struct{ data []byte }
type RCBktReq struct{ data []byte }
type RCReplyBktReq struct{ data []byte }
type RCTriggerReq struct{ data []byte }

func (req RCTxnReq) GetMsgType() RC_REQ_TYPE        { return RC_TXN }
func (req RCClkReq) GetMsgType() RC_REQ_TYPE        { return RC_CLK }
func (req RCIdReq) GetMsgType() RC_REQ_TYPE         { return RC_ID }
func (req RCJoinReq) GetMsgType() RC_REQ_TYPE       { return RC_JOIN }
func (req RCReplyJoinReq) GetMsgType() RC_REQ_TYPE  { return RC_REPLY_JOIN }
func (req RCReplyEmptyReq) GetMsgType() RC_REQ_TYPE { return RC_EMPTY_REPLY }
func (req RCBktReq) GetMsgType() RC_REQ_TYPE        { return RC_REQ_BKT }
func (req RCReplyBktReq) GetMsgType() RC_REQ_TYPE   { return RC_REPLY_BKT }
func (req RCTriggerReq) GetMsgType() RC_REQ_TYPE    { return RC_TRIGGER }
func (req RCTxnReq) GetID() int32                   { return req.reqId }
func (req RCClkReq) GetID() int32                   { return req.reqId }
func (req RCIdReq) GetID() int32                    { return 0 } //Not used
func (req RCJoinReq) GetID() int32                  { return 0 } //Not used
func (req RCReplyJoinReq) GetID() int32             { return 0 } //Not used
func (req RCReplyEmptyReq) GetID() int32            { return 0 } //Not used
func (req RCBktReq) GetID() int32                   { return 0 } //Not used
func (req RCReplyBktReq) GetID() int32              { return 0 } //Not used
func (req RCTriggerReq) GetID() int32               { return 0 } //Not used

func ReqCompFunc(a, b RCSendRequest) bool {
	return a.GetID() < b.GetID()
}

type RC_REQ_TYPE int32

const (
	RC_TXN RC_REQ_TYPE = iota
	RC_CLK
	RC_ID
	RC_JOIN
	RC_REPLY_JOIN
	RC_REQ_BKT
	RC_REPLY_BKT
	RC_EMPTY_REPLY
	RC_TRIGGER
)

const INITIAL_PER_PART_BUCKET_LEN = 10 //Initial slice of the buffer holding upds for a given bucket.
const HOLD_PER_PART_BUFF_LEN = 100

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
	bigTopicPrefix    = "h."
	triggerTopic      = "trigger"
	remoteIDContent   = "remoteID"
	joinContent       = "join"
	replyJoinContent  = "replyJoin"
	requestBktContent = "requestBkt"
	replyBktContent   = "replyBkt"
	replyEmptyContent = "replyEmpty"
	//replQueueName     = "repl"
	joinQueueName = "join"
	MAX_MSG_SIZE  = 128 * 1024 * 1024 //128MB
)

var (
	basePrefix, baseVHost string
)

func CreateRemoteConnStruct(ip string, bucketsToListen []string, replicaID uint16, connID uint16, isSelfConn bool, workChan chan RCWork) (remote *RemoteConn) {
	remote = createRemoteConnStructHelper(ip, bucketsToListen, replicaID, connID, isSelfConn, workChan)
	prefix := basePrefix + ":" + basePrefix + "@"
	link := protocol + prefix + ip + "/" + baseVHost
	if isSelfConn { //We only replicate txns in our connection.
		go remote.handleReplicatorReqs() //Safe to start this early, as this will only marshall and prepare the txn for sending, not actually send it.
	}
	go remote.connectToRabbitMQ(link, ip, isSelfConn)

	return
}

func createRemoteConnStructHelper(ip string, bucketsToListen []string, replicaID uint16, connID uint16, isSelfConn bool, workChan chan RCWork) (remote *RemoteConn) {
	allBuckets := false
	//conn, err := amqp.Dial(protocol + prefix + ip)
	bucketsMap := make(map[string]struct{}, len(bucketsToListen))
	for _, bkt := range bucketsToListen {
		bucketsMap[bkt] = struct{}{}
		if bkt == "*" {
			allBuckets = true
		}
	}
	holdTxn := HoldTxn{onHold: tools.NewSliceWithCounter[RemoteTxn](len(bucketsToListen)), partBuf: make([]tools.SliceWithCounter[crdt.UpdateObjectParams], nGoRoutines), partsInvolved: tools.NewBitSet(int(nGoRoutines))}
	for i := 0; i < int(nGoRoutines); i++ {
		holdTxn.partBuf[i] = tools.NewSliceWithCounter[crdt.UpdateObjectParams](HOLD_PER_PART_BUFF_LEN)
	}
	var replReqChan chan RCWork
	if isSelfConn { //Only self connection will receive replication requests from the Replicator.
		replReqChan = make(chan RCWork, 100)
	}
	return &RemoteConn{
		listenerChan:     make(chan ReplicatorMsg, 10),
		replicaID:        replicaID,
		replicaString:    fmt.Sprint(replicaID),
		HoldTxn:          holdTxn,
		nBucketsToListen: int32(len(bucketsToListen)),
		buckets:          bucketsMap,
		connID:           connID,
		allBuckets:       allBuckets,
		workChan:         workChan,
		connEstablished:  false,
		replReqChan:      replReqChan,
		senderRoutineCh:  make(chan RCSendRequest, 100),
		replDebugData:    replDebugData{debugChan: make(chan any, 100)},
	}
}

// Only returns after the connection is established
func CreateRemoteConnStructWithWait(ip string, bucketsToListen []string, replicaID uint16, connID uint16, isSelfConn bool, workChan chan RCWork) (remote *RemoteConn) {
	remote = createRemoteConnStructHelper(ip, bucketsToListen, replicaID, connID, isSelfConn, workChan)
	prefix := basePrefix + ":" + basePrefix + "@"
	link := protocol + prefix + ip + "/" + baseVHost
	remote.connectToRabbitMQ(link, ip, isSelfConn)
	return
}

func (remote *RemoteConn) connectToRabbitMQ(link, ip string, isSelfConn bool) {
	timeString := time.Now().Format("15:04:05.000")
	fmt.Printf("[RC][%s]Connecting to %s\n", timeString, link)
	conn, err := amqp.DialConfig(link, amqp.Config{Dial: scalateTimeout})
	nAttempts, nSleepTime := 0, 50*time.Millisecond
	for err != nil { //It actually managed to connect but RabbitMQ throw an error. Try again quickly. However, given this always happens to local, also add a small delay each time.
		timeString = time.Now().Format("15:04:05.000")
		nAttempts++
		if nAttempts%10 == 0 {
			fmt.Printf("[RC][%s]Failed to open connection to rabbitMQ at %s: %s. Attempts so far: %d. Retrying.\n", timeString, ip, err, nAttempts)
		}
		//utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to open connection to rabbitMQ at", ip, ":", err, ". Retrying.")
		//time.Sleep(4000 * time.Millisecond)
		time.Sleep(nSleepTime)
		nSleepTime += (50 * time.Millisecond)
		conn, err = amqp.Dial(link)
	}
	timeString = time.Now().Format("15:04:05.000")
	fmt.Printf("[RC][%s]Connected to %s\n", timeString, link)
	remote.conn = conn
	remote.finishInitialization(isSelfConn, ip)
}

func (remote *RemoteConn) finishInitialization(isSelfConn bool, ip string) {
	sendCh, err := remote.conn.Channel()
	if err != nil {
		fmt.Printf("[RC][FATAL]Failed to obtain channel from rabbitMQ: %s\n", err)
		//utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to obtain channel from rabbitMQ:", err)
		panic(err)
	}

	//Call this to delete existing exchange/queues/binds/etc if configurations are changed
	//deleteRabbitMQStructures(sendCh)

	//We send msgs to the exchange
	err = sendCh.ExchangeDeclare(exchangeName, exchangeType, false, false, false, false, nil)
	if err != nil {
		fmt.Printf("[RC][FATAL]Failed to declare exchange with rabbitMQ: %s\n", err)
		//utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to declare exchange with rabbitMQ:", err)
		panic(err)
	}
	//This queue will store messages sent from other replicas
	replQueue, err := sendCh.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		fmt.Printf("[RC][FATAL]Failed to declare queue with rabbitMQ: %s\n", err)
		//utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to declare queue with rabbitMQ:", err)
		panic(err)
	}
	joinQueue, err := sendCh.QueueDeclare(joinQueueName, false, true, false, false, nil)
	if err != nil {
		fmt.Printf("[RC][FATAL]Failed to declare join queue with rabbitMQ: %s\n", err)
		//utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to declare join queue with rabbitMQ:", err)
		panic(err)
	}
	//The previously declared queue will receive messages from any replica that publishes updates for objects in buckets in bucketsToListen
	//bucket.* - matches anything with "bucket.(any word). * means one word, any"
	//bucket.# - matches anything with "bucket.(any amount of words). # means 0 or more words, each separated by a dot."
	for bucket := range remote.buckets {
		sendCh.QueueBind(replQueue.Name, bucketTopicPrefix+bucket, exchangeName, false, nil)
		//For groups
		sendCh.QueueBind(replQueue.Name, groupTopicPrefix+bucket, exchangeName, false, nil)
		//For split messages
		sendCh.QueueBind(replQueue.Name, bigTopicPrefix+bucketTopicPrefix+bucket, exchangeName, false, nil)
		//For split messages
		sendCh.QueueBind(replQueue.Name, bigTopicPrefix+groupTopicPrefix+bucket, exchangeName, false, nil)
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
	//fmt.Printf("[RC%d]Queues names: %s, %s, %s\n", connID, replQueue.Name, joinQueue.Name, queueToListen.Name)
	recCh, err = sendCh.Consume(queueToListen.Name, "", true, false, false, false, nil)
	if err != nil {
		fmt.Printf("[RC][FATAL]Failed to obtain consumer from rabbitMQ: %s\n", err)
		//utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to obtain consumer from rabbitMQ:", err)
		panic(err)
	}
	if !isSelfConn {
		fmt.Println("[RC]Listening to repl on connection to", ip, "with id", remote.connID)
	} else {
		fmt.Println("[RC]Listening to join on connection to", ip, "with id", remote.connID)
	}
	remote.sendCh, remote.recCh = sendCh, recCh
	go remote.startReceiver()
	go remote.doSenderRoutine()
}

func scalateTimeout(network, addr string) (conn net.Conn, err error) {
	//Also control the timeout locally due to the localhost case which returns immediatelly
	nextTimeout := 100 * time.Millisecond
	lastAttempt := time.Now().UnixNano()
	timeSincePrint := int64(0)
	nAttempts := 0
	for {
		conn, err = net.DialTimeout(network, addr, nextTimeout)
		if err == nil {
			break
		} else {
			if nAttempts == 0 { //Force a longer first sleep
				time.Sleep(2500 * time.Millisecond)
			}
			nAttempts++
			//Controlling timeout locally
			thisAttempt := time.Now().UnixNano()
			if thisAttempt-lastAttempt < int64(nextTimeout) {
				time.Sleep(nextTimeout - time.Duration(thisAttempt-lastAttempt))
			}
			timeSincePrint += (thisAttempt - lastAttempt) / int64(time.Millisecond)
			lastAttempt = thisAttempt
			if nextTimeout < 1000 {
				nextTimeout += 50
			} else if nextTimeout < 1500 {
				nextTimeout += 100
			} else {
				nextTimeout += (nextTimeout / 4)
			}
			if timeSincePrint > 5000 {
				fmt.Printf("[RC][ScalateTimeout function]Failed to connect to %s %d times. Retrying...\n", addr, nAttempts)
				timeSincePrint = 0
			}
			//fmt.Println("[RC][ScalateTimeout function]Failed to connect to", addr, ". Retrying to connect...")
		}
	}
	if conn == nil {
		fmt.Printf("[RC][ScalateTimeout function][SERIOUS ERROR]Failed to connect to %s GIVING UP ON THIS CONNECTION! THIS IS UNEXPECTED. Error: %s\n", addr, err)
	}
	return
}

// Note: This should *ONLY* be used when a declaration of one of RabbitMQ structures (exchange, queue, etc.) changes
func deleteRabbitMQStructures(ch *amqp.Channel) {
	//Also deletes queue binds
	ch.ExchangeDelete(exchangeName, false, false)
}

// Just for signature.
func (work PrepareTxnWork) DoWork(replicaID uint16) {}

func (remote *RemoteConn) SendTxn(txn RemoteTxn) {
	remote.replCount++
	remote.replReqChan <- PrepareTxnWork{txn: txn, reqId: remote.replCount}
}

func (remote *RemoteConn) SendStableClk(ts int64) {
	remote.replCount++
	protobuf := createProtoStableClock(remote.replicaID, ts)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of stableClk request to send. Error:", err)
	}
	remote.senderRoutineCh <- RCClkReq{data: data, reqId: remote.replCount}
}

func (remote *RemoteConn) SendTrigger(trigger AutoUpdate, isGeneric bool) {
	//Create new gob encoder (TODO: Try making this a variable that is used ONLY for triggers)
	//Also note that this TODO won't work with new replicas for sure.

	ci := CodingInfo{}.EncInitialize()
	protobuf := CreateNewTrigger(trigger, isGeneric, ci)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of trigger request to send. Error:", err)
	}
	remote.senderRoutineCh <- RCTriggerReq{data: data}
}

func (remote *RemoteConn) doSenderRoutine() {
	//Txn and Clk messages are forced to be sent by order. Join and similar are naturally sent by order too, as the requester is single-threaded and sends directly to this channel.
	lastSentReq := int32(0)
	waitingReqs := tools.NewHeap[RCSendRequest](ReqCompFunc, 10)
	for msg := range remote.senderRoutineCh {
		switch typedMsg := msg.(type) {
		case RCTxnReq:
			if msg.GetID() > lastSentReq+1 {
				waitingReqs.Push(msg)
			} else {
				lastSentReq++
				bkts, datas := typedMsg.bktTxn.GetKeys(), typedMsg.bktTxn.GetValues()
				for i, bkt := range bkts {
					remote.sendMsg(bkt, datas[i])
				}
				lastSentReq = remote.handleWaitingReqs(waitingReqs, lastSentReq)
			}
		case RCClkReq:
			if msg.GetID() > lastSentReq+1 {
				waitingReqs.Push(msg)
			} else {
				lastSentReq++
				remote.publishHelper(clockTopic, amqp.Publishing{CorrelationId: remote.replicaString, Body: typedMsg.data})
				lastSentReq = remote.handleWaitingReqs(waitingReqs, lastSentReq)
			}
			remote.publishHelper(clockTopic, amqp.Publishing{CorrelationId: remote.replicaString, Body: typedMsg.data})
		case RCIdReq:
			remote.publishHelper(joinTopic, amqp.Publishing{CorrelationId: remote.replicaString, ContentType: remoteIDContent, Body: typedMsg.data})
		case RCJoinReq:
			fmt.Println("[RC]Sending/queing join as", remote.replicaID, remote.connID)
			remote.publishHelper(joinTopic, amqp.Publishing{CorrelationId: remote.replicaString, ContentType: joinContent, Body: typedMsg.data})
		case RCReplyJoinReq:
			remote.publishHelper(joinTopic, amqp.Publishing{CorrelationId: remote.replicaString, ContentType: replyJoinContent, Body: typedMsg.data})
		case RCReplyEmptyReq:
			remote.publishHelper(joinTopic, amqp.Publishing{CorrelationId: remote.replicaString, ContentType: replyEmptyContent, Body: typedMsg.data})
		case RCBktReq:
			remote.publishHelper(joinTopic, amqp.Publishing{CorrelationId: remote.replicaString, ContentType: requestBktContent, Body: typedMsg.data})
		case RCReplyBktReq:
			remote.publishHelper(joinTopic, amqp.Publishing{CorrelationId: remote.replicaString, ContentType: replyBktContent, Body: typedMsg.data})
		case RCTriggerReq:
			remote.publishHelper(triggerTopic, amqp.Publishing{CorrelationId: remote.replicaString, Body: typedMsg.data})
		}
	}
}

func (remote *RemoteConn) handleWaitingReqs(waitingReqs *tools.Heap[RCSendRequest], lastSentReq int32) (newLastSentReq int32) {
	for !waitingReqs.IsEmpty() {
		next := waitingReqs.PeekMin()
		if next.GetID() == lastSentReq+1 {
			waitingReqs.Pop()
			lastSentReq++
			switch typedMsg := next.(type) {
			case RCTxnReq:
				bkts, datas := typedMsg.bktTxn.GetKeys(), typedMsg.bktTxn.GetValues()
				for i, bkt := range bkts {
					remote.sendMsg(bkt, datas[i])
				}
			case RCClkReq:
				remote.publishHelper(clockTopic, amqp.Publishing{CorrelationId: remote.replicaString, Body: typedMsg.data})
			}
		} else {
			break
		}
	}
	return lastSentReq
}

func (remote *RemoteConn) handleReplicatorReqs() {
	nRoutines := tools.Max(2, tools.Min(10, runtime.NumCPU()/16))
	go remote.debugCollectStatistics()
	for i := 0; i < nRoutines; i++ {
		go remote.handleReplicatorReqsRoutine(i)
	}
}

func (remote *RemoteConn) handleReplicatorReqsRoutine(id int) {
	bktBuf := make([]map[string]*tools.SliceWithCounter[crdt.UpdateObjectParams], nGoRoutines)
	//We slightly randomize the ticker frequency for cleaning buffers, in an attempt to get different routines to trigger this at different times.
	ticker := time.NewTicker(time.Duration(tools.Max(10000, int(tsSendDelay)*2)+rand.Intn(100)*100) * time.Millisecond)
	if !remote.allBuckets {
		for i := uint64(0); i < nGoRoutines; i++ {
			currPart := make(map[string]*tools.SliceWithCounter[crdt.UpdateObjectParams], len(remote.buckets))
			for bucket := range remote.buckets {
				currPart[bucket] = tools.NewSliceWithCounterPointer[crdt.UpdateObjectParams](INITIAL_PER_PART_BUCKET_LEN)
			}
			bktBuf[i] = currPart
		}
	}
	hasHadTxns := true
	for {
		select {
		case req := <-remote.replReqChan:
			switch typedReq := req.(type) {
			case PrepareTxnWork:
				fmt.Printf("[RC%d][handleReplicatorReqsRoutine%d]Received PrepareTxnWork req, preparing txn with clk %s and %d parts.\n", remote.connID, id, typedReq.txn.Clk.ToString(), len(typedReq.txn.Upds))
				remote.prepareTxn(typedReq.txn, typedReq.reqId, bktBuf)
			}
		case <-ticker.C:
			if hasHadTxns && len(remote.replReqChan) == 0 { //Skip ticker if there's still pending requests, as then we'll overwrite part of the buffer anyway.
				for i := uint64(0); i < nGoRoutines; i++ {
					for _, slice := range bktBuf[i] {
						slice.DeepClear()
					}
				}
			}
		}
	}
}

func (remote *RemoteConn) debugCollectStatistics() {
	nTxnsFullyProc, totalSeqTime := int64(0), int64(0)
	lastNSplit, lastNToProto, lastNPrepRec := int64(0), int64(0), int64(0)              //StatisticsTxnPrep
	lastNProtoCreation, lastNMarshall, lastNMarshallRec := int64(0), int64(0), int64(0) //StatisticsMarshall
	nTxnsFullyRec, totalRecTime := int64(0), int64(0)
	lastNRec, lastNRecUnmarshall, lastNRecConvertTime, lastNRecMergeTime := int64(0), int64(0), int64(0), int64(0) //StatisticsTxnRec and StatisticsTxnRecComplete
	ms := int64(time.Millisecond)
	for info := range remote.replDebugData.debugChan {
		switch typedInfo := info.(type) {
		case StatisticsTxnPrep:
			nTxnsFullyProc++
			lastNSplit += typedInfo.splitTime / ms
			lastNToProto += typedInfo.toProtoTotalTime / ms
			totalSeqTime += (typedInfo.splitTime + typedInfo.toProtoTotalTime) / ms
			lastNPrepRec++
			if lastNPrepRec == 100 { //Print and reset lastN statistics
				avgSplit, avgToProto := float64(lastNSplit)/float64(lastNPrepRec), float64(lastNToProto)/float64(lastNPrepRec)
				avgProtoCreation, avgMarshall := float64(lastNProtoCreation)/float64(lastNMarshallRec), float64(lastNMarshall)/float64(lastNMarshallRec)
				avgTotalTimeLast := avgSplit + avgToProto
				fullAvgTime := float64(totalSeqTime) / float64(nTxnsFullyProc)
				currTime := time.Now()
				//Note: statistics from the marshalling process may be regarding more or less than 100 txns. Also, it's the average per bucket of a txn.
				fmt.Printf("[RC%d][Debug][%s]Processed %d txns. Last 100 txn statistics.\n Avg split time: %.2f ms.\n Avg toProto time: %.2f ms.\n Avg last 100 full proc time: %.2f ms.\n Avg proto (per bkt) creation time: %.2f ms.\n Avg marshall (per bkt) time: %.2f ms.\n Avg total time since start: %.2f ms.\n\n",
					remote.connID, currTime.Format("2006-01-02 15:04:05"), nTxnsFullyProc, avgSplit, avgToProto, avgTotalTimeLast, avgProtoCreation, avgMarshall, fullAvgTime)
				lastNSplit, lastNToProto, lastNPrepRec = 0, 0, 0
				lastNProtoCreation, lastNMarshall, lastNMarshallRec = 0, 0, 0
			}
		case StatisticsMarshall:
			lastNProtoCreation += typedInfo.protoCreationTime / ms
			lastNMarshall += typedInfo.marshallTime / ms
			lastNMarshallRec++

		case StatisticsTxnRec:
			lastNRecUnmarshall += typedInfo.unmarshallTime / ms
			lastNRecConvertTime += typedInfo.convertTime / ms
			totalRecTime += (typedInfo.unmarshallTime + typedInfo.convertTime) / ms
		case StatisticsTxnRecComplete:
			nTxnsFullyRec++
			lastNRecMergeTime += typedInfo.mergeTime / ms
			totalRecTime += typedInfo.mergeTime / ms
			lastNRec++
			if lastNRec == 100 { //Print and reset lastN statistics
				avgUnmarshall, avgConvert, avgMerge := float64(lastNRecUnmarshall)/float64(lastNRec), float64(lastNRecConvertTime)/float64(lastNRec), float64(lastNRecMergeTime)/float64(lastNRec)
				avgTotalTimeLast := avgUnmarshall + avgConvert + avgMerge
				fullAvgTime := float64(totalRecTime) / float64(nTxnsFullyRec)
				currTime := time.Now()
				fmt.Printf("[RC%d][Debug][%s]Processed %d txns. Last 100 txn statistics.\n Avg unmarshall time: %.2f ms.\n Avg convert time: %.2f ms.\n Avg merge time: %.2f ms.\n Avg last 100 full proc time: %.2f ms.\n Avg total time since start: %.2f ms.\n\n",
					remote.connID, currTime.Format("2006-01-02 15:04:05"), nTxnsFullyRec, avgUnmarshall, avgConvert, avgMerge, avgTotalTimeLast, fullAvgTime)
				lastNRec, lastNRecUnmarshall, lastNRecConvertTime, lastNRecMergeTime = 0, 0, 0, 0
			}
		}
	}
}

func (remote *RemoteConn) prepareTxn(txn RemoteTxn, reqId int32, bktBuf []map[string]*tools.SliceWithCounter[crdt.UpdateObjectParams]) {
	start := time.Now().UnixNano()
	txn.TxnID = reqId
	bktTxn := remote.splitTxnIntoBuckets(txn, bktBuf)
	endSplit := time.Now().UnixNano()
	fmt.Printf("[RC%d][prepareTxn%d]Split txn of %d parts into %d buckets, took %dms\n", remote.connID, reqId, len(txn.Upds), len(bktTxn), (endSplit-start)/int64(time.Millisecond))
	replyChan, waitFor := make(chan PairKeyBytes, len(bktTxn)), len(bktTxn)
	for bkt, bktTxns := range bktTxn {
		fmt.Printf("[RC%d][prepareTxn%d]Sending txn to bucket %s with %d parts, senderID %d, clk %s\n", remote.connID, reqId, bkt, len(bktTxns.Upds), txn.SenderID, txn.Clk.ToString())
		remote.workChan <- ReplMarshallWork{Bucket: bkt, Txn: bktTxns, ReplyChan: replyChan}
	}
	//We must ensure the sender will send txns by order. It's easier to manage this if we gather all buckets of this txn here first.
	//(In theory only the order within a bucket must be ensured, but this complicates a lot the whole replication logic with little to no benefit)
	replies := tools.NewSliceMap[string, []byte](len(bktTxn))
	for ; waitFor > 0; waitFor-- {
		reply := <-replyChan
		replies.SetNew(reply.Key, reply.Data)
	}
	end := time.Now().UnixNano()
	remote.senderRoutineCh <- RCTxnReq{bktTxn: replies, reqId: reqId}
	remote.debugChan <- StatisticsTxnPrep{splitTime: endSplit - start, toProtoTotalTime: end - endSplit}
}

func (remote *RemoteConn) splitTxnIntoBuckets(txn RemoteTxn, bktBuf []map[string]*tools.SliceWithCounter[crdt.UpdateObjectParams]) (bktTxn map[string]RemoteTxn) {
	//Do we want to send this per pair (topic, partition), or per topic?
	//Honestly it's up to us! But would make sense to have a topic have all relevant partitions.
	if remote.allBuckets { //Since we don't know which buckets we replicate, we may need to add more buckets on the fly.
		for partID, upds := range txn.Upds {
			currPart := bktBuf[partID]
			for _, upd := range upds {
				bkt := upd.Bucket
				slice, has := currPart[bkt]
				if !has {
					slice = tools.NewSliceWithCounterPointer[crdt.UpdateObjectParams](INITIAL_PER_PART_BUCKET_LEN)
					currPart[bkt] = slice
				}
				slice.Append(upd)
			}
		}
		bktTxn = make(map[string]RemoteTxn)
		for partID, currPart := range bktBuf {
			for bkt, upds := range currPart {
				if upds.Len() > 0 {
					txn, has := bktTxn[bkt]
					if !has {
						txn = RemoteTxn{SenderID: txn.SenderID, Clk: txn.Clk, Upds: make(map[int][]crdt.UpdateObjectParams, nGoRoutines), TxnID: txn.TxnID}
						bktTxn[bkt] = txn
					}
					txn.Upds[partID] = upds.Copy().ToSlice()
					upds.Clear()
				}
			}
		}
	} else {
		//Note: Tracking which buckets are used would be very expensive, as we'd need to access a map for every upd's bucket. So better at the end we just delete empty positions.
		//(This would be easy to solve if we were able to convert bucket into ints or similar before getting to this point)
		for partID, upds := range txn.Upds {
			currPart := bktBuf[partID]
			for _, upd := range upds {
				bkt := upd.Bucket
				currPart[bkt].Append(upd)
			}
		}
		bktTxn = make(map[string]RemoteTxn, len(remote.buckets))
		for bkt := range remote.buckets {
			bktTxn[bkt] = RemoteTxn{SenderID: txn.SenderID, Clk: txn.Clk, Upds: make(map[int][]crdt.UpdateObjectParams, nGoRoutines), TxnID: txn.TxnID}
		}
		for partID, currPart := range bktBuf {
			for bkt, upds := range currPart {
				if upds.Len() > 0 {
					bktTxn[bkt].Upds[partID] = upds.Copy().ToSlice()
					upds.Clear()
				}
			}
		}
		for bkt, txn := range bktTxn { //Often, not every bucket will be updated.
			if len(txn.Upds) == 0 {
				delete(bktTxn, bkt)
			}
		}
	}
	return
}

// Idea: can send any message through a special "big data" interface
// On the first message, we also include the total size of the protobuf
// The idea is that the client, after receiving the first big message, he can keep track of how much more he has received
// So after first msg, wait for the next ones. Make some kind of cache per server/church.
func (remote *RemoteConn) sendMsg(key string, data []byte) {
	//start := time.Now()
	if len(data) <= MAX_MSG_SIZE {
		//fmt.Printf("[RC%d][SendMsg]Starting to send txn as a single msg. Size of msg: %.2f (MB). Started at: %s.\n", remote.connID, float64(len(data))/float64(1024*1024), start.Format("2006-01-02 15:04:05.000"))
		remote.publishHelper(key, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		//remote.sendCh.Publish(exchangeName, key, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		/*end := time.Now()
		fmt.Printf("[RC][SendMsg]Finished sending txn as a single msg. Size of msg: %.3f (MB). Started at: %s. Finished at: %s. Time taken: %d (ms)\n", float64(len(data))/float64(1024*1024),
			start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), (end.UnixNano()-start.UnixNano())/1000000)*/
	} else {
		remote.publishHelper(bigTopicPrefix+key, amqp.Publishing{CorrelationId: remote.replicaString, AppId: strconv.Itoa(len(data)), Body: data[0:MAX_MSG_SIZE]})
		//remote.sendCh.Publish(exchangeName, bigTopicPrefix+key, false, false, amqp.Publishing{CorrelationId: remote.replicaString, AppId: strconv.Itoa(len(data)), Body: data[0:MAX_MSG_SIZE]})
		j := 1
		totalSent := MAX_MSG_SIZE
		//fmt.Printf("[RC%d][SendMsg]Sent part %d of txn. Size of curr msg: %d (%.2f MB). Size sent so far: %d (%.2f MB). Total size: %d (%.2f MB)\n",
		//	remote.connID, j, totalSent, float64(totalSent)/float64(1024*1024), totalSent, float64(totalSent)/float64(1024*1024), len(data), float64(len(data))/float64(1024*1024))
		leftData := data[MAX_MSG_SIZE:]
		for len(leftData) > 0 {
			toSend := leftData
			if len(toSend) > MAX_MSG_SIZE {
				toSend = leftData[:MAX_MSG_SIZE]
			}
			remote.publishHelper(bigTopicPrefix+key, amqp.Publishing{CorrelationId: remote.replicaString, Body: toSend})
			//remote.sendCh.Publish(exchangeName, bigTopicPrefix+key, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: toSend})
			totalSent += len(toSend)
			j++
			//fmt.Printf("[RC%d][SendMsg]Sent part %d of txn. Size of curr msg: %d (%.2f MB). Size sent so far: %d (%.2f MB). Total size: %d (%.2f MB)\n",
			//	remote.connID, j, len(toSend), float64(len(toSend))/float64(1024*1024), totalSent, float64(totalSent)/float64(1024*1024), len(data), float64(len(data))/float64(1024*1024))
			leftData = leftData[utilities.MinInt(MAX_MSG_SIZE, len(leftData)):]
		}
	}
}

func (remote *RemoteConn) publishHelper(topic string, publish amqp.Publishing) {
	remote.sendCh.Publish(exchangeName, topic, false, false, publish)
}

// This should not be called externally.
func (remote *RemoteConn) startReceiver() {
	//fmt.Println("[RC]Receiver started")
	nTxnReceived := 0
	for data := range remote.recCh {
		//utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Received something!")
		//fmt.Printf("[RC%d]Receiving something (%s) at: %s\n", remote.connID, data.RoutingKey, time.Now().String())
		switch data.RoutingKey {
		case clockTopic:
			remote.handleReceivedStableClock(data.Body)
		case joinTopic:
			remote.handleReceivedJoinTopic(data.ContentType, data.Body)
		case triggerTopic:
			remote.handleReceivedTrigger(data.Body)
		default:
			if strings.HasPrefix(data.RoutingKey, bigTopicPrefix) {
				//fmt.Printf("[RC%d]Received split message with size %d (%.2f MB) out of %s\n",
				//	remote.connID, len(data.Body), float64(len(data.Body))/float64(1024*1024), data.AppId)
				remote.receiveSplitMsg(data)
			} else {
				remote.handleReceivedOps(data.Body)
			}
			nTxnReceived++
		}
		//fmt.Printf("[RC%d]Finished receiving something at: %s\n", remote.connID, time.Now().String())
		//}
	}
}

func (remote *RemoteConn) receiveSplitMsg(data amqp.Delivery) {
	//fmt.Printf("[RC%d]Starting to merge back split message.\n", remote.connID)
	done := false
	fullSize, _ := strconv.Atoi(data.AppId)
	//fmt.Printf("[RC%d]Expecting to receive a total size of split message of: %d (%.2f MB)\n", remote.connID, fullSize, float64(fullSize)/float64(1024*1024))
	buf, currI := make([]byte, fullSize), len(data.Body)
	//fmt.Printf("[RC%d]Received a size of %d (%.2f MB). Total received so far: %d (%.2f MB)\n", remote.connID,
	//	len(data.Body), float64(len(data.Body))/float64(1024*1024), len(data.Body), float64(len(data.Body))/float64(1024*1024))
	//startTime := time.Now().UnixNano() / 1000000
	copy(buf, data.Body)
	//endTime := time.Now().UnixNano() / 1000000
	//totalTime := endTime - startTime
	for !done {
		//fmt.Printf("[RC%d]Forcing to receive split messages.\n", remote.connID)
		data = <-remote.recCh
		//startTime = time.Now().UnixNano() / 1000000
		copy(buf[currI:], data.Body)
		//endTime = time.Now().UnixNano() / 1000000
		//totalTime += endTime - startTime
		currI += len(data.Body)
		//fmt.Printf("[RC%d]Received on receiveSplitMsg: %s. With size: %d (%0.2f MB). Total size received: %d (%0.2f MB). Expected size: %d (%0.2f MB)\n",
		//remote.connID, data.RoutingKey, len(data.Body), float64(len(data.Body))/float64(1024*1024), currI, float64(currI)/float64(1024*1024),
		//fullSize, float64(fullSize)/float64(1024*1024))
		if currI == fullSize {
			//fmt.Printf("[RC%d]Finished receiving split message, going out of cycle.\n", remote.connID)
			done = true
		}
	}
	//fmt.Println("[RC]Time taken merging back split message: ", totalTime, "ms")
	//fmt.Printf("[RC%d]Finished merging back split message.\n", remote.connID)
	//remote.debugChan <- tool
	remote.handleReceivedOps(buf)
}

func (remote *RemoteConn) handleReceivedOps(data []byte) {
	//fmt.Printf("[RC%d]HandleReceivedOps called for data with size %d\n", remote.connID, len(data))
	protobuf := &proto.ProtoReplicateTxn{}
	start := time.Now().UnixNano()
	err := pb.Unmarshal(data, protobuf)
	endMarshall := time.Now().UnixNano()
	if err != nil {
		fmt.Printf("[RC%d][ERROR]Failed to decode bytes of received ProtoReplicateTxn. Error: %s\n", remote.connID, err)
		os.Exit(0)
	} /*else {
		fmt.Printf("[RC%d]Sucessfully decoded to protobuf data with size %d\n", remote.connID, len(data))
	}*/
	//Each transaction must be hold until we receive all buckets
	//This is identified by receiving another transaction or a clock.
	bktTxn := protoToRemoteTxn(protobuf)
	end := time.Now().UnixNano()
	remote.debugChan <- StatisticsTxnRec{unmarshallTime: endMarshall - start, convertTime: end - endMarshall}
	/*for i, upds := range bktTxn.Upds {
		for _, upd := range upds {
			fmt.Printf("[RC%d][handleReceivedOps]Received updates!!! Key: %s. Partition: %d.\n", remote.connID, upd.KeyParams, i)
		}
	}*/
	//fmt.Printf("[RC%d]Received txn %d\n", remote.connID, bktTxn.TxnID)
	if bktTxn.TxnID != remote.txnID {
		//Need to send the previous txn that is now complete
		remote.sendMerged()
		remote.createHold(bktTxn.TxnID)
	}
	remote.storeTxn(bktTxn)
}

func (remote *RemoteConn) handleReceivedStableClock(data []byte) {
	protobuf := &proto.ProtoStableClock{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received stableClock. Error:", err)
	}
	clkReq := protoToStableClock(protobuf)
	utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Received remote stableClock:", clkReq)
	if clkReq.SenderID == remote.replicaID {
		fmt.Println("[RC][Warning]Received clock from self - is localrabbitmq correctly configured?")
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Ignored received stableClock as it was sent by myself.")
	} else {
		remote.sendMerged() //No-op if there's no txn on hold. This will also clear the hold if needed.
		remote.listenerChan <- clkReq
	}
}

func (remote *RemoteConn) sendMerged() {
	if !remote.onHold.IsEmpty() { //It may be empty after a clock is received without txns inbetween.
		remote.listenerChan <- remote.getMergedTxn()
	}
}

func (remote *RemoteConn) getMergedTxn() (merged *RemoteTxn) {
	start := time.Now().UnixNano()
	var currReq RemoteTxn
	if remote.onHold.Len() == 1 { //We use directly the (only) bucket txn's buffers
		currReq = remote.onHold.Get(0)
		merged = &currReq
	} else { //Need to merge multiple buffers.
		merged = &RemoteTxn{}
		//merged.Upds = make(map[int][]crdt.UpdateObjectParams, remote.partsInvolved.GetNBitsSet(len(remote.partBuf)))
		holdSlice := remote.onHold.ToSlice()
		for _, req := range holdSlice {
			for partID, partUpds := range req.Upds {
				remote.partBuf[partID].AppendAll(partUpds)
				remote.partsInvolved.Set(partID)
			}
		}
		merged.Upds = make(map[int][]crdt.UpdateObjectParams, remote.partsInvolved.GetNBitsSet(int(nGoRoutines)))
		merged.SenderID, merged.Clk = currReq.SenderID, currReq.Clk
	}
	remote.clearHold()
	end := time.Now().UnixNano()
	remote.debugChan <- StatisticsTxnRecComplete{mergeTime: end - start}
	return
}

func (remote *RemoteConn) createHold(txnID int32) {
	remote.txnID = txnID
}

func (remote *RemoteConn) clearHold() {
	remote.onHold.Clear()
	for i := range remote.partBuf {
		remote.partBuf[i].Clear()
	}
	remote.partsInvolved.Reset()
}

func (remote *RemoteConn) storeTxn(txn RemoteTxn) {
	remote.onHold.Append(txn)
}

//Trigger + join logic.

func (remote *RemoteConn) GetNextRemoteRequest() (request ReplicatorMsg) {
	//Wait until all operations for a partition arrive. We can detect this in two ways:
	//1st - the next operation we receive is for a different partition
	//2nd - the next operation is a clock update
	return <-remote.listenerChan
}

/***** JOINING LOGIC *****/

func (remote *RemoteConn) SendRemoteID(idData []byte) {
	/*err := remote.sendCh.Publish(exchangeName, joinTopic, false, false,
		amqp.Publishing{CorrelationId: remote.replicaString, ContentType: remoteIDContent, Body: idData})
	if err != nil {
		fmt.Printf("[RC%d]Error sending remoteID: %s\n", remote.connID, err)
		panic(err)
	}*/
	//remote.publishHelper(joinTopic, amqp.Publishing{CorrelationId: remote.replicaString, ContentType: remoteIDContent, Body: idData})
	remote.senderRoutineCh <- RCIdReq{data: idData}
}

func (remote *RemoteConn) SendJoin(joinData []byte) {
	fmt.Println("[RC]Sending/queing join as", remote.replicaID, remote.connID)
	remote.senderRoutineCh <- RCJoinReq{data: joinData}
}

func (remote *RemoteConn) SendReplyJoin(req ReplyJoin) {
	protobuf := createProtoReplyJoin(req)
	data := remote.encodeProtobuf(protobuf, "Failed to generate bytes of ReplyJoin msg.")
	remote.senderRoutineCh <- RCReplyJoinReq{data: data}
}

func (remote *RemoteConn) SendRequestBucket(req RequestBucket) {
	protobuf := createProtoRequestBucket(req)
	data := remote.encodeProtobuf(protobuf, "Failed to generate bytes of RequestBucket msg.")
	remote.senderRoutineCh <- RCBktReq{data: data}
}

func (remote *RemoteConn) SendReplyBucket(req ReplyBucket) {
	protobuf := createProtoReplyBucket(req)
	data := remote.encodeProtobuf(protobuf, "Failed to generate bytes of ReplyBucket msg.")
	remote.senderRoutineCh <- RCReplyBktReq{data: data}
}

func (remote *RemoteConn) SendReplyEmpty() {
	protobuf := createProtoReplyEmpty()
	data := remote.encodeProtobuf(protobuf, "Failed to generate bytes of ReplyEmpty msg.")
	remote.senderRoutineCh <- RCReplyEmptyReq{data: data}
}

func (remote *RemoteConn) handleReceivedTrigger(data []byte) {
	protobuf, ci := &proto.ApbNewTrigger{}, CodingInfo{}.DecInitialize()
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received trigger. Error:", err)
	}
	trigger := ProtoTriggerToAntidote(protobuf, ci)
	remote.listenerChan <- &RemoteTrigger{AutoUpdate: trigger, IsGeneric: protobuf.GetIsGeneric()}
}

func (remote *RemoteConn) handleReceivedJoinTopic(msgType string, data []byte) {
	switch msgType {
	case remoteIDContent:
		//fmt.Println("Join msg is a remoteID")
		remote.handleRemoteID(data)
	case joinContent:
		//fmt.Println("Join msg is a join")
		remote.handleJoin(data)
	case replyJoinContent:
		//fmt.Println("Join msg is a replyJoin")
		remote.handleReplyJoin(data)
	case requestBktContent:
		//fmt.Println("Join msg is a requestBkt")
		remote.handleRequestBkt(data)
	case replyBktContent:
		//fmt.Println("Join msg is a replyBkt")
		remote.handleReplyBkt(data)
	case replyEmptyContent:
		//fmt.Println("Join msg is a replyEmpty")
		remote.handleReplyEmpty(data)
	default:
		fmt.Printf("Unexpected join msg, ignored: %s\n", msgType)
	}
}

func (remote *RemoteConn) handleRemoteID(data []byte) {
	protobuf := &proto.ProtoRemoteID{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoRemoteID. Error:")
	senderID := uint16(protobuf.GetReplicaID())
	fmt.Println("[RC]Join msg is a remoteID. RemoteID from:", senderID, remote.connID, "at", time.Now().Format("15:04:05.000"))
	remote.listenerChan <- RemoteID{SenderID: senderID, Buckets: protobuf.GetMyBuckets(), IP: protobuf.GetMyIP()}
}

func (remote *RemoteConn) handleJoin(data []byte) {
	protobuf := &proto.ProtoJoin{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoJoin. Error:")
	otherBkts, senderID, ip := protoToJoin(protobuf)
	fmt.Println("[RC]Join msg is a join. Join from:", senderID, remote.connID)

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
		remote.listenerChan <- Join{SenderID: senderID, ReplyID: remote.connID, CommonBkts: commonBkts, ReqIP: ip}
	} else {
		//TODO: Ask group to send empty
	}
}

func (remote *RemoteConn) handleReplyJoin(data []byte) {
	protobuf := &proto.ProtoReplyJoin{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoReplyJoin. Error:")
	buckets, clks, senderID := protoToReplyJoin(protobuf)
	fmt.Println("[RC]Join msg is a replyJoin. ReplyJoin from:", senderID, remote.connID, remote.sendCh)
	remote.listenerChan <- ReplyJoin{SenderID: senderID, ReplyID: remote.connID, Clks: clks, CommonBkts: buckets, ReqIP: protobuf.GetReplicaIP()}
}

func (remote *RemoteConn) handleRequestBkt(data []byte) {
	protobuf := &proto.ProtoRequestBucket{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoRequestBucket. Error:")
	buckets, senderID := protoToRequestBucket(protobuf)
	fmt.Println("[RC]Join msg is a requestBkt. RequestBkt from:", senderID, remote.connID)
	remote.listenerChan <- RequestBucket{SenderID: senderID, ReplyID: remote.connID, Buckets: buckets, ReqIP: protobuf.GetReplicaIP()}
}

func (remote *RemoteConn) handleReplyBkt(data []byte) {
	protobuf := &proto.ProtoReplyBucket{}
	remote.decodeProtobuf(protobuf, data, "Failed to decode bytes of received protoReplyBucket. Error:")
	states, clk, senderID := protoToReplyBucket(protobuf)
	fmt.Println("[RC]Join msg is a replyBkt. ReplyBkt from:", senderID, remote.connID)
	remote.listenerChan <- ReplyBucket{SenderID: senderID, PartStates: states, Clk: clk}
}

func (remote *RemoteConn) handleReplyEmpty(data []byte) {
	remote.listenerChan <- ReplyEmpty{}
}

func (remote *RemoteConn) decodeProtobuf(protobuf pb.Message, data []byte, errMsg string) {
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, errMsg, err)
	}
}

func (remote *RemoteConn) encodeProtobuf(protobuf pb.Message, errMsg string) (data []byte) {
	data, err := pb.Marshal(protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, errMsg, err)
	}
	return
}

func (remote *RemoteConn) checkProtoError(err error, msg pb.Message, upds map[int][]crdt.UpdateObjectParams) {
	switch typedProto := msg.(type) {
	case *proto.ProtoReplicateTxn:
		fmt.Println("[RC]Error creating ProtoReplicateTxn.")
		fmt.Printf("%+v\n", typedProto)
		fmt.Println(upds)
		fmt.Println("[RC]Timestamp:", (clocksi.SliceTimestamp{}.FromBytes(typedProto.GetTimestamp())).ToSortedString())
	}
}
