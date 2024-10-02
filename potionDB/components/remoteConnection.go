package components

import (
	fmt "fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"potionDB/potionDB/utilities"

	//pb "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
	pb "google.golang.org/protobuf/proto"
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
	allBuckets       bool        //True if the replica's buckets has the wildcard '*' (i.e., replicates every bucket)
	workChan         chan RCWork //Channel to send requests of marshalling.
	//SendMsgLock      NillMutex   //sync.Mutex
}

/*type NillMutex struct{}

func (m NillMutex) Lock()   {}
func (m NillMutex) Unlock() {}*/

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
	upds  map[int][]crdt.UpdateObjectParams //Updates per partition of a bucket of one txn
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

// Topics (i.e., queue filters): partitionID.bucket
// There's one queue per replica. Each replica's queue will receive messages from ALL other replicas, as long as the topics match the ones
// that were binded to the replica's queue.
// Ip includes both ip and port in format: ip:port
// In the case of the connection to the local replica's RabbitMQ server, we don't consume the self messages (isSelfConn)
func CreateRemoteConnStruct(ip string, bucketsToListen []string, replicaID int16, connID int16, isSelfConn bool, workChan chan RCWork) (remote *RemoteConn, err error) {
	//conn, err := amqp.Dial(protocol + prefix + ip + port)
	allBuckets := false
	prefix := basePrefix + ":" + basePrefix + "@"
	//conn, err := amqp.Dial(protocol + prefix + ip)
	link := protocol + prefix + ip + "/" + baseVHost
	currH, currM, currS := time.Now().Clock()
	fmt.Printf("[RC][%d:%d:%d]Connecting to %s\n", currH, currM, currS, link)
	conn, err := amqp.DialConfig(link, amqp.Config{Dial: scalateTimeout})
	if err != nil {
		currH, currM, currS = time.Now().Clock()
		fmt.Printf("[RC][%d:%d:%d]Failed to open connection to rabbitMQ at %s: %s. Retrying.\n", currH, currM, currS, ip, err)
		utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to open connection to rabbitMQ at", ip, ":", err, ". Retrying.")
		time.Sleep(4000 * time.Millisecond)
		return CreateRemoteConnStruct(ip, bucketsToListen, replicaID, connID, isSelfConn, workChan)
	}
	sendCh, err := conn.Channel()
	if err != nil {
		utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to obtain channel from rabbitMQ:", err)
		return nil, err
	}

	//Call this to delete existing exchange/queues/binds/etc if configurations are changed
	//deleteRabbitMQStructures(sendCh)

	//We send msgs to the exchange
	//sendCh.ExchangeDelete(exchangeName, false, true)
	err = sendCh.ExchangeDeclare(exchangeName, exchangeType, false, false, false, false, nil)
	if err != nil {
		utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to declare exchange with rabbitMQ:", err)
		return nil, err
	}
	//This queue will store messages sent from other replicas
	replQueue, err := sendCh.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to declare queue with rabbitMQ:", err)
		return nil, err
	}
	joinQueue, err := sendCh.QueueDeclare(joinQueueName, false, true, false, false, nil)
	if err != nil {
		utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to declare join queue with rabbitMQ:", err)
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
	fmt.Printf("[RC%d]Queues names: %s, %s, %s\n", connID, replQueue.Name, joinQueue.Name, queueToListen.Name)
	recCh, err = sendCh.Consume(queueToListen.Name, "", true, false, false, false, nil)
	if err != nil {
		utilities.FancyWarnPrint(utilities.REMOTE_PRINT, replicaID, "failed to obtain consumer from rabbitMQ:", err)
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
		workChan:         workChan,
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

// Note: This should *ONLY* be used when a declaration of one of RabbitMQ structures (exchange, queue, etc.) changes
func deleteRabbitMQStructures(ch *amqp.Channel) {
	//Also deletes queue binds
	ch.ExchangeDelete(exchangeName, false, false)
}

/*
type groupReplicateInfo struct {
	bucketOps map[string][]map[int][]crdt.UpdateObjectParams

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
		var currTxnBuckets map[string]map[int][]crdt.UpdateObjectParams
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
		var currTxnBuckets map[string]map[int][]crdt.UpdateObjectParams
		for _, txn := range txns {
			currTxnBuckets = remote.txnToBuckets(txn)
			for bkt, partUpds := range currTxnBuckets {
				bucketOps[bkt][*pos[bkt]] = RemoteTxn{SenderID: txn.SenderID, Clk: txn.Clk, TxnID: txn.TxnID, Upds: partUpds}
				*pos[bkt]++

				/*for i, upds := range partUpds {
					for _, upd := range upds {
						fmt.Printf("[RC][SendGroupTxn]Trying to send view updates!!! Key: %s. Partition: %d.\n", upd.KeyParams, i)
					}
				}*/
			}
		}
	}

	/*
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
				//remote.sendCh.Publish(exchangeName, groupTopicPrefix+bkt, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
				remote.sendMsg(groupTopicPrefix+bkt, data)
			}
		}*/

	replyChan, waitFor := make(chan PairKeyBytes, len(bucketOps)), 0
	for bkt, bktTxns := range bucketOps {
		if *pos[bkt] > 0 {
			waitFor++
			work := GroupMarshallWork{Bucket: bkt, Txns: txns, BktTxns: bktTxns[:*pos[bkt]], ReplyChan: replyChan}
			remote.workChan <- work
		}
	}
	for ; waitFor > 0; waitFor-- {
		result := <-replyChan
		remote.sendMsg(groupTopicPrefix+result.Key, result.Data)
	}
	remote.txnCount += int32(len(txns))
}

/*
func (remote *RemoteConn) SendGroupTxn(txns []RemoteTxn) {
	bucketOps := make(map[string][]map[int][]crdt.UpdateObjectParams) //bucket -> txn -> part -> upds
	for bkt := range remote.buckets {
		bucketOps[bkt] = make([]map[int][]crdt.UpdateObjectParams, len(txns))
	}
	//var currTxnBuckets map[string]BktOpsIdPair
	var currTxnBuckets map[string]map[int][]crdt.UpdateObjectParams
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

type PairKeyBytes struct {
	Key  string
	Data []byte
}

func (remote *RemoteConn) SendTxnsIndividually(txns []RemoteTxn) {
	//start := time.Now()
	replyChan := make(chan MarshallWorkReply, len(txns))
	offset := remote.txnCount
	for _, txn := range txns {
		txn.TxnID = remote.txnCount
		bucketOps := remote.txnToBuckets(txn)
		work := MarshallWork{BucketOps: bucketOps, Txn: txn, ReplyChan: replyChan}
		remote.workChan <- work
		remote.txnCount++
		/*for i, upds := range txn.Upds {
			for _, upd := range upds {
				fmt.Printf("[RC][SendTxnsIndividually]Trying to send view updates!!! Key: %s. Partition: %d.\n", upd.KeyParams, i)
			}
		}*/
	}
	workBuf := make([]MarshallWorkReply, len(txns))
	for i := 0; i < len(txns); i++ {
		workReply := <-replyChan
		pos := workReply.TxnSendId - offset
		workBuf[pos] = workReply
		if pos == 0 {
			for len(workBuf[0].Result) > 0 { //Checking if already received
				work := workBuf[0]
				for _, pair := range work.Result {
					remote.sendMsg(pair.Key, pair.Data)
				}
				if len(workBuf) > 1 {
					workBuf = workBuf[1:]
					offset++
				} else {
					break
				}
			}
		}
	}
	/*end := time.Now()
	fmt.Printf("[RC][SendTxnsIndividually]Finished sending %d txns. Started at: %s. End time: %s. Total time taken: %d (ms)\n",
		len(txns), start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), (end.UnixNano()-start.UnixNano())/1000000)*/
}

/*
func (remote *RemoteConn) SendTxn(txn RemoteTxn) {
	start := time.Now()
	fmt.Printf("[RC][SendTxn]Preparing to send txn to RabbitMQ. TxnClk: %s. Started at: %s\n", txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"))
	txn.TxnID = remote.txnCount
	//First, get the txn separated in buckets and, for each bucket, in partitions
	bucketOps := remote.txnToBuckets(txn)
	fmt.Printf("[RC][SendTxn]Finished splitting txn into buckets. TxnClk: %s. Started at: %s. Current time: %s.\n",
		txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), time.Now().Format("2006-01-02 15:04:05.000"))
	//Now, build the protos
	finishChan := make(chan PairKeyBytes, len(bucketOps))
	for bucket, upds := range bucketOps {
		go func(bucket string, upds map[int][]crdt.UpdateObjectParams) {
			protobuf := createProtoReplicateTxn(remote.replicaID, txn.Clk, upds, remote.txnCount)
			fmt.Printf("[RC][SendTxn]Finished creating protoReplicateTxn. TxnClk: %s. Started at: %s. Current time: %s.\n",
				txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), time.Now().Format("2006-01-02 15:04:05.000"))
			data, err := pb.Marshal(protobuf)
			fmt.Printf("[RC][SendTxn]Finished marshalling protoReplicateTxn. TxnClk: %s. Started at: %s. Current time: %s.\n",
				txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), time.Now().Format("2006-01-02 15:04:05.000"))
			if err != nil {
				remote.checkProtoError(err, protobuf, upds)
				os.Exit(0)
			}
			//fmt.Printf("[RC%d]Sending individual txn %d for bucket %s.\n", remote.connID, remote.txnCount, bucket)
			//remote.sendCh.Publish(exchangeName, strconv.FormatInt(int64(remote.txnCount), 10)+"."+bucket, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
			//remote.sendCh.Publish(exchangeName, bucketTopicPrefix+bucket, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
			//remote.sendMsg(bucketTopicPrefix+bucket, data)
			finishChan <- PairKeyBytes{Key: bucketTopicPrefix + bucket, Data: data}
			end := time.Now()
			fmt.Printf("[RC][SendTxn]Finished sending protoReplicateTxn. TxnClk: %s. Started at: %s. End time: %s. Total time taken: %d\n",
				txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), (end.UnixNano()-start.UnixNano())/1000000)
		}(bucket, upds)
	}
	for i := 0; i < len(bucketOps); i++ {
		pair := <-finishChan
		remote.sendMsg(pair.Key, pair.Data)
	}
	remote.txnCount++
	end := time.Now()
	fmt.Printf("[RC][SendTxn]Finished sending all buckets. TxnClk: %s Started at: %s. End time: %s. Total time taken: %d\n",
		txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), (end.UnixNano()-start.UnixNano())/1000000)
}*/

// Idea: can send any message through a special "big data" interface
// On the first message, we also include the total size of the protobuf
// The idea is that the client, after receiving the first big message, he can keep track of how much more he has received
// So after first msg, wait for the next ones. Make some kind of cache perserver/church.
func (remote *RemoteConn) sendMsg(key string, data []byte) {
	//start := time.Now()
	if len(data) <= MAX_MSG_SIZE {
		//fmt.Printf("[RC%d][SendMsg]Starting to send txn as a single msg. Size of msg: %.2f (MB). Started at: %s.\n", remote.connID, float64(len(data))/float64(1024*1024), start.Format("2006-01-02 15:04:05.000"))
		remote.sendCh.Publish(exchangeName, key, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		/*end := time.Now()
		fmt.Printf("[RC][SendMsg]Finished sending txn as a single msg. Size of msg: %.3f (MB). Started at: %s. Finished at: %s. Time taken: %d (ms)\n", float64(len(data))/float64(1024*1024),
			start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), (end.UnixNano()-start.UnixNano())/1000000)*/
	} else {
		remote.sendCh.Publish(exchangeName, bigTopicPrefix+key, false, false, amqp.Publishing{CorrelationId: remote.replicaString, AppId: strconv.Itoa(len(data)), Body: data[0:MAX_MSG_SIZE]})
		//j := 1
		//totalSent := MAX_MSG_SIZE
		//fmt.Printf("[RC%d][SendMsg]Sent part %d of txn. Size of curr msg: %d (%.2f MB). Size sent so far: %d (%.2f MB). Total size: %d (%.2f MB)\n",
		//remote.connID, j, totalSent, float64(totalSent)/float64(1024*1024), totalSent, float64(totalSent)/float64(1024*1024), len(data), float64(len(data))/float64(1024*1024))
		leftData := data[MAX_MSG_SIZE:]
		for len(leftData) > 0 {
			toSend := leftData
			if len(toSend) > MAX_MSG_SIZE {
				toSend = leftData[:MAX_MSG_SIZE]
			}
			remote.sendCh.Publish(exchangeName, bigTopicPrefix+key, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: leftData})
			//totalSent += len(toSend)
			//j++
			//fmt.Printf("[RC%d][SendMsg]Sent part %d of txn. Size of curr msg: %d (%.2f MB). Size sent so far: %d (%.2f MB). Total size: %d (%.2f MB)\n",
			//remote.connID, j, len(toSend), float64(len(toSend))/float64(1024*1024), totalSent, float64(totalSent)/float64(1024*1024), len(data), float64(len(data))/float64(1024*1024))
			leftData = leftData[utilities.MinInt(MAX_MSG_SIZE, len(leftData)):]
		}
	}
}

func (remote *RemoteConn) txnToBuckets(txn RemoteTxn) (bucketOps map[string]map[int][]crdt.UpdateObjectParams) {
	//We need to send one message per bucket. Note that we're sending operations out of order here - this might be relevant later on!
	//(the order for operations in the same bucket is kept however)
	//ProtoReplicateTxn: one txn; slice of partitions
	//ProtoNewRemoteTxn: one txn of a partition.
	//Re-arrange the operations into groups of buckets, with each bucket having entries for each partition
	bucketOps = make(map[string]map[int][]crdt.UpdateObjectParams) //bucket -> part -> upds
	for partID, partUpds := range txn.Upds {
		for _, upd := range partUpds {
			entry, hasEntry := bucketOps[upd.KeyParams.Bucket]
			if !hasEntry {
				entry = make(map[int][]crdt.UpdateObjectParams)
				bucketOps[upd.KeyParams.Bucket] = entry
			}
			partEntry, hasEntry := entry[partID]
			if !hasEntry {
				partEntry = make([]crdt.UpdateObjectParams, 0, len(partUpds))
			}
			partEntry = append(partEntry, upd)
			entry[partID] = partEntry
		}
	}
	return
}

func (remote *RemoteConn) checkProtoError(err error, msg pb.Message, upds map[int][]crdt.UpdateObjectParams) {
	switch typedProto := msg.(type) {
	case *proto.ProtoReplicateTxn:
		fmt.Println("[RC]Error creating ProtoReplicateTxn.")
		fmt.Printf("%+v\n", typedProto)
		fmt.Println(upds)
		fmt.Println("[RC]Timestamp:", (clocksi.ClockSiTimestamp{}.FromBytes(typedProto.GetTimestamp())).ToSortedString())
	}
}

/*
func (remote *RemoteConn) SendPartTxn(request *NewReplicatorRequest) {
	utilities.FancyDebugPrint(utilities.REMOTE_PRINT, remote.replicaID, "Sending remote request:", *request)
	if len(request.Upds) > 0 {
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Actually sending txns to other replicas. Sending ops:", request.Upds)
	}

	//We need to send one message per bucket. Note that we're sending operations out of order here - this might be relevant later on!
	bucketOps := make(map[string][]crdt.UpdateObjectParams)
	for _, upd := range request.Upds {
		entry, hasEntry := bucketOps[upd.KeyParams.Bucket]
		if !hasEntry {
			entry = make([]crdt.UpdateObjectParams, 0, len(request.Upds))
		}
		entry = append(entry, upd)
		bucketOps[upd.KeyParams.Bucket] = entry
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Entry upds:", entry)
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Map's entry upds:", bucketOps[upd.KeyParams.Bucket])
	}

	for bucket, upds := range bucketOps {
		protobuf := createProtoReplicatePart(request.SenderID, request.PartitionID, request.Timestamp, upds, remote.txnCount)
		data, err := pb.Marshal(protobuf)
		//_, err := pb.Marshal(protobuf)
		if err != nil {
			utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of partTxn request to send. Error:", err)
			fmt.Println(protobuf)
			fmt.Println(upds)
			fmt.Println("Timestamp:", request.Timestamp)
			downUpdsProtos := protobuf.GetTxn().GetUpds()
			for _, proto := range downUpdsProtos {
				fmt.Println("Upd proto:", proto)
			}
		}
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Sending bucket ops to topic:", strconv.FormatInt(request.PartitionID, 10)+"."+bucket)
		fmt.Printf("[RC][%d]Sending txn at %s\n", remote.debugCount, time.Now().String())
		remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.PartitionID, 10)+"."+bucket, false, false, amqp.Publishing{CorrelationId: remote.replicaString, Body: data})
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Finished sending bucket ops to topic:", strconv.FormatInt(request.PartitionID, 10)+"."+bucket)
		remote.debugCount++
	}
	remote.txnCount++
}
*/

func (remote *RemoteConn) SendStableClk(ts int64) {
	utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Sending stable clk:", ts)
	protobuf := createProtoStableClock(remote.replicaID, ts)
	data, err := pb.Marshal(protobuf)
	//_, err := pb.Marshal(protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of stableClk request to send. Error:", err)
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
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of trigger request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, triggerTopic, false, false, amqp.Publishing{Body: data})
}

// This should not be called externally.
func (remote *RemoteConn) startReceiver() {
	fmt.Println("[RC]Receiver started")
	nTxnReceived := 0
	for data := range remote.recCh {
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Received something!")
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
				remote.receiveSplitMsg(data)
			} else if strings.HasPrefix(data.RoutingKey, groupTopicPrefix) {
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

func (remote *RemoteConn) receiveSplitMsg(data amqp.Delivery) {
	//fmt.Printf("[RC%d]Starting to merge back split message.\n", remote.connID)
	done := false
	fullSize, _ := strconv.Atoi(data.AppId)
	//fmt.Printf("[RC%d]Expecting to receive a total size of split message of: %d (%.2f MB)\n", remote.connID, fullSize, float64(fullSize)/float64(1024*1024))
	buf, currI := make([]byte, fullSize), len(data.Body)
	//fmt.Printf("[RC%d]Received a size of %d (%.2f MB). Total received so far: %d (%.2f MB)", remote.connID,
	//len(data.Body), float64(len(data.Body))/float64(1024*1024), len(data.Body), float64(len(data.Body))/float64(1024*1024))
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
	key := data.RoutingKey[2:]
	if strings.HasPrefix(key, groupTopicPrefix) {
		// Group
		remote.handleReceivedGroupOps(buf)
	} else {
		remote.handleReceivedOps(buf)
	}
}

func (remote *RemoteConn) handleReceivedGroupOps(data []byte) {
	protobuf := &proto.ProtoReplicateGroupTxn{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		fmt.Printf("[RC%d][ERROR]Failed to decode bytes of received ProtoReplicateGroupTxn. Error: %v", remote.connID, err)
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
	//fmt.Printf("[RC%d]HandleReceivedOps called for data with size %d\n", remote.connID, len(data))
	protobuf := &proto.ProtoReplicateTxn{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		fmt.Printf("[RC%d][ERROR]Failed to decode bytes of received ProtoReplicateTxn. Error: %s\n", remote.connID, err)
		os.Exit(0)
	} /*else {
		fmt.Printf("[RC%d]Sucessfully decoded to protobuf data with size %d\n", remote.connID, len(data))
	}*/
	//Each transaction must be hold until we receive all buckets
	//This is identified by receiving another transaction or a clock.
	bktTxn := protoToRemoteTxn(protobuf)
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
	merged.Upds = make(map[int][]crdt.UpdateObjectParams)
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
	currMergedUpds := make(map[int][]crdt.UpdateObjectParams)
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
					potencialSkip = utilities.MinInt32(currRemoteTxn.TxnID, potencialSkip)
				}
			}
		}
		if len(currMergedUpds) > 0 {
			merged.Txns = append(merged.Txns, RemoteTxn{SenderID: merged.SenderID, Clk: currClk, Upds: currMergedUpds, TxnID: currID})
			currID++
			currMergedUpds = make(map[int][]crdt.UpdateObjectParams)
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
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received request. Error:", err)
	}
	request := protoToReplicatorRequest(protobuf)
	//remote.decBuf.Reset()
	utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Received remote request:", *request)
	if len(request.Upds) > 0 && request.SenderID != remote.replicaID {
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Actually received txns from other replicas.")
	}
	if request.SenderID != remote.replicaID {
		holdOps, hasHold := remote.holdOperations[request.SenderID]
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Processing received remote transactions.")
		if !hasHold {
			//Initial case
			utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Initial case - Creating hold for remote ops.")
			remote.holdOperations[request.SenderID] = remote.buildHoldOperations(request)
		} else if holdOps.txnID == request.TxnID {
			//Hold the received operations
			utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Adding remote ops to hold.")
			holdOps.lastRecUpds = append(holdOps.lastRecUpds, request)
			holdOps.nOpsSoFar += len(request.Upds)
		} else {
			//This request corresponds to a different partition from the one we have cached. As such, we can send the cached partition's updates
			utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Different partitions - sending previous ops to replicator and creating hold for the ones received now.")
			reqToSend := remote.getMergedReplicatorRequest(holdOps, request.SenderID)
			remote.holdOperations[request.SenderID] = remote.buildHoldOperations(request)
			remote.listenerChan <- reqToSend
		}
	} else {
		fmt.Println("[RC]Received ops from self - is localRabbitMQ instance correctly configured?")
		utilities.FancyDebugPrint(utilities.REMOTE_PRINT, remote.replicaID, "Ignored request from self.")
	}
	//utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "My replicaID:", remote.replicaID, "senderID:", request.SenderID)
}
*/

func (remote *RemoteConn) handleReceivedStableClock(data []byte) {
	protobuf := &proto.ProtoStableClock{}
	err := pb.Unmarshal(data, protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received stableClock. Error:", err)
	}
	clkReq := protoToStableClock(protobuf)
	utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Received remote stableClock:", *clkReq)
	if clkReq.SenderID == remote.replicaID {
		fmt.Println("[RC]Received clock from self - is localrabbitmq correctly configured?")
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Ignored received stableClock as it was sent by myself.")
	} else {
		/*
			//We also need to send a request with the last partition ops, if there's any
			utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Processing received stableClock.")
			holdOperations, hasOps := remote.holdOperations[clkReq.SenderID]
			if hasOps {
				utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Merging previous requests before sending stableClock to replicator.")
				partReq := remote.getMergedReplicatorRequest(holdOperations, clkReq.SenderID)
				remote.listenerChan <- partReq
			} else {
				utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "No previous request.")
			}
			utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Sending stableClock to replicator.")
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
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received trigger. Error:", err)
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
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of trigger request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, triggerTopic, false, false, amqp.Publishing{Body: data})
}
*/

/*
func (remote *RemoteConn) getMergedReplicatorRequest(holdOps *HoldOperations, replicaID int16) (request *NewReplicatorRequest) {
	utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Merging received replicator requests")
	request = &NewReplicatorRequest{
		PartitionID: holdOps.partitionID,
		SenderID:    replicaID,
		Timestamp:   holdOps.lastRecUpds[0].Timestamp,
		Upds:        make([]crdt.UpdateObjectParams, 0, holdOps.nOpsSoFar),
		TxnID:       holdOps.txnID,
	}
	for _, reqs := range holdOps.lastRecUpds {
		utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Merging upds:", reqs.Upds)
		request.Upds = append(request.Upds, reqs.Upds...)
	}
	delete(remote.holdOperations, replicaID)
	utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Upds in merged request:", request.Upds)
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
	utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Hold after creation:", *holdOps)
	utilities.FancyInfoPrint(utilities.REMOTE_PRINT, remote.replicaID, "Ops of request added to hold:", *holdOps.lastRecUpds[0])
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
