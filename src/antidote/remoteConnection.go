package antidote

import (
	"reflect"
	"strconv"
	"tools"

	proto "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

type RemoteConn struct {
	conn             *amqp.Connection
	sendCh           *amqp.Channel
	recCh            <-chan amqp.Delivery
	listenerChan     chan ReplicatorMsg
	replicaID        int64
	holdOperations   map[int64]*HoldOperations
	nBucketsToListen int
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
	prefix = "guest:guest@"
	//port         = "5672/"
	exchangeName = "objRepl"
	exchangeType = "topic"
	//Go back to using this to buffer requests if we stop using remoteGroup
	//defaultListenerSize = 100
	clockTopic = "clk"
)

//Topics (i.e., queue filters): partitionID.bucket
//There's one queue per replica. Each replica's queue will receive messages from ALL other replicas, as long as the topics match the ones
//that were binded to the replica's queue.
//Ip includes both ip and port in format: ip:port
func CreateRemoteConnStruct(ip string, bucketsToListen []string, replicaID int64) (remote *RemoteConn, err error) {
	//conn, err := amqp.Dial(protocol + prefix + ip + port)
	conn, err := amqp.Dial(protocol + prefix + ip)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to open connection to rabbitMQ:", err)
		return nil, err
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
	queue, err := sendCh.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to declare queue with rabbitMQ:", err)
		return nil, err
	}
	//The previously declared queue will receive messages from any replica that publishes updates for objects in buckets in bucketsToListen
	//TODO: Probably remove the partition part? (i.e., the *.)
	for _, bucket := range bucketsToListen {
		sendCh.QueueBind(queue.Name, "*."+bucket, exchangeName, false, nil)
	}
	//We also need to listen to stable clocks.
	//TODO: Some kind of filtering for this
	sendCh.QueueBind(queue.Name, clockTopic, exchangeName, false, nil)

	//This channel is used to read from the queue
	//Note: the true corresponds to auto ack. We should *probably* do manual ack later on
	recCh, err := sendCh.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to obtain consumer from rabbitMQ:", err)
		return nil, err
	}

	remote = &RemoteConn{
		sendCh: sendCh,
		conn:   conn,
		recCh:  recCh,
		//listenerChan:     make(chan ReplicatorMsg, defaultListenerSize),
		listenerChan:     make(chan ReplicatorMsg),
		replicaID:        replicaID,
		holdOperations:   make(map[int64]*HoldOperations),
		nBucketsToListen: len(bucketsToListen),
	}
	go remote.startReceiver()
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
		for _, upd := range request.Upds {
			tools.FancyWarnPrint(tools.REMOTE_PRINT, remote.replicaID, "Downstream args:", upd, "type:", reflect.TypeOf(upd.UpdateArgs))
		}
	}

	//We need to send one message per bucket. Note that we're sending operations out of order here - this might be relevant later on!
	bucketOps := make(map[string][]UpdateObjectParams)
	for _, upd := range request.Upds {
		entry, hasEntry := bucketOps[upd.KeyParams.Bucket]
		if !hasEntry {
			entry = make([]UpdateObjectParams, 0, len(request.Upds))
		}
		entry = append(entry, upd)
		bucketOps[upd.KeyParams.Bucket] = entry
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Entry upds:", entry)
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Map's entry upds:", bucketOps[upd.KeyParams.Bucket])
	}

	for bucket, upds := range bucketOps {
		protobuf := createProtoReplicatePart(request.SenderID, request.PartitionID, request.Timestamp, upds)
		data, err := proto.Marshal(protobuf)
		if err != nil {
			tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of partTxn request to send. Error:", err)
		}
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending bucket ops to topic:", strconv.FormatInt(request.PartitionID, 10)+"."+bucket)
		remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.PartitionID, 10)+"."+bucket, false, false, amqp.Publishing{Body: data})
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Finished sending bucket ops to topic:", strconv.FormatInt(request.PartitionID, 10)+"."+bucket)
	}
}

func (remote *RemoteConn) SendStableClk(ts int64) {
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending stable clk:", ts)
	protobuf := createProtoStableClock(remote.replicaID, ts)
	data, err := proto.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of stableClk request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, clockTopic, false, false, amqp.Publishing{Body: data})
}

/*
func (remote *RemoteConn) SendReplicatorRequest(request *NewReplicatorRequest) {
	tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending remote request:", *request)
	if len(request.Txns) > 0 {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually sending txns to other replicas.")
	}
	protobuf := createProtoReplicatePart(request)
	data, err := proto.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.PartitionID, 10), false, false, amqp.Publishing{Body: data})
}
*/

//This should not be called externally.
func (remote *RemoteConn) startReceiver() {
	for data := range remote.recCh {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Received something!")
		//TODO: Maybe analyze data.routingKey to avoid decoding the protobuf if it was sent by this own replica? RoutingKey no longer has information about the sender though...
		if data.RoutingKey == clockTopic {
			remote.handleReceivedStableClock(data.Body)
		} else {
			remote.handleReceivedOps(data.Body)
		}
	}
}

func (remote *RemoteConn) handleReceivedOps(data []byte) {
	protobuf := &ProtoReplicatePart{}
	err := proto.Unmarshal(data, protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received request. Error:", err)
	}
	request := protoToReplicatorRequest(protobuf)
	//remote.decBuf.Reset()
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote request:", *request)
	if len(request.Upds) > 0 && request.SenderID != remote.replicaID {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually received txns from other replicas.")
		for _, upd := range request.Upds {
			tools.FancyWarnPrint(tools.REMOTE_PRINT, remote.replicaID, "Received downstream args:", upd, "type:", reflect.TypeOf(upd.UpdateArgs))
		}
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
		tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Ignored request from self.")
	}
	//tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "My replicaID:", remote.replicaID, "senderID:", request.SenderID)
}

func (remote *RemoteConn) handleReceivedStableClock(data []byte) {
	protobuf := &ProtoStableClock{}
	err := proto.Unmarshal(data, protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received stableClock. Error:", err)
	}
	clkReq := protoToStableClock(protobuf)
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote stableClock:", *clkReq)
	//TODO: Avoid receiving own messages?
	if clkReq.SenderID == remote.replicaID {
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

func (remote *RemoteConn) getMergedReplicatorRequest(holdOps *HoldOperations, replicaID int64) (request *NewReplicatorRequest) {
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Merging received replicator requests")
	request = &NewReplicatorRequest{
		PartitionID: holdOps.partitionID,
		SenderID:    replicaID,
		Timestamp:   holdOps.lastRecUpds[0].Timestamp,
		Upds:        make([]UpdateObjectParams, 0, holdOps.nOpsSoFar),
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

//This should not be called externally.
/*
func (remote *RemoteConn) startReceiver() {
	for data := range remote.recCh {
		protobuf := &ProtoReplicatePart{}
		err := proto.Unmarshal(data.Body, protobuf)
		if err != nil {
			tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received request. Error:", err)
		}
		request := protoToReplicatorRequest(protobuf)
		//remote.decBuf.Reset()
		tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote request:", *request)
		if len(request.Txns) > 0 {
			tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually received txns from other replicas.")
		}
		if request.SenderID == remote.replicaID {
			tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote request from myself!")
		}
		//tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "My replicaID:", remote.replicaID, "senderID:", request.SenderID)
		remote.listenerChan <- request
	}
}
*/

func (remote *RemoteConn) GetNextRemoteRequest() (request ReplicatorMsg) {
	//Wait until all operations for a partition arrive. We can detect this in two ways:
	//1st - the next operation we receive is for a different partition
	//2nd - the next operation is a clock update
	return <-remote.listenerChan
}

/*
func (remote *RemoteConn) SendReplicatorRequest(request *NewReplicatorRequest) {
	tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending remote request:", *request)
	if len(request.Txns) > 0 {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually sending txns to other replicas.")
	}
	protobuf := createProtoReplicatePart(request)
	data, err := proto.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.PartitionID, 10), false, false, amqp.Publishing{Body: data})
}
*/

/*

import (
	"bytes"
	"clocksi"
	"crdt"
	"encoding/gob"
	"strconv"
	"tools"

	"github.com/streadway/amqp"
)

type RemoteConn struct {
	conn         *amqp.Connection
	sendCh       *amqp.Channel
	recQueue     amqp.Queue
	recCh        <-chan amqp.Delivery
	encBuf       *bytes.Buffer
	decBuf       *bytes.Buffer
	byteBuf      *bytes.Buffer
	encoder      *gob.Encoder
	decoder      *gob.Decoder
	listenerChan chan *ReplicatorRequest
	replicaID    int64 //TODO: Remove, as this is not necessary. It's just here for debugging purposes
}

const (
	protocol            = "amqp://"
	ip                  = "guest:guest@localhost:"
	port                = "5672/"
	exchangeName        = "objRepl"
	exchangeType        = "direct"
	defaultListenerSize = 100
)

//TODO: Remove replicaID, it's just here for debugging purposes
func CreateRemoteConnStruct(replicaID int64) (remote *RemoteConn, err error) {
	conn, err := amqp.Dial(protocol + ip + port)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to open connection to rabbitMQ:", err)
		return nil, err
	}
	sendCh, err := conn.Channel()
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to obtain channel from rabbitMQ:", err)
		return nil, err
	}
	//We send msgs to the exchange
	//sendCh.ExchangeDelete(exchangeName, false, true)
	err = sendCh.ExchangeDeclare(exchangeName, exchangeType, false, false, false, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to declare exchange with rabbitMQ:", err)
		return nil, err
	}
	//This queue will store messages sent from other replicas
	queue, err := sendCh.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to declare queue with rabbitMQ:", err)
		return nil, err
	}
	//As soon as we know the other replicas, we need to bind the queue to their IDs, in order to receive their msgs.
	//Which is done by calling "listenToReplica(id)"

	//This channel is used to read from the queue
	//Note: the true corresponds to auto ack. We should *probably* do manual ack later on
	recCh, err := sendCh.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		tools.FancyWarnPrint(tools.REMOTE_PRINT, replicaID, "failed to obtain consumer from rabbitMQ:", err)
		return nil, err
	}

	//initializing gob. Need to register all types which will be used as interface implementations
	gob.Register(clocksi.DummyTs)
	for _, crdt := range crdt.DummyCRDTs {
		args := crdt.GetPossibleDownstreamTypes()
		for _, arg := range args {
			gob.Register(arg)
		}
	}

	var encBuf, decBuf bytes.Buffer
	remote = &RemoteConn{
		sendCh:       sendCh,
		conn:         conn,
		recQueue:     queue,
		recCh:        recCh,
		encBuf:       &encBuf,
		decBuf:       &decBuf,
		encoder:      gob.NewEncoder(&encBuf),
		decoder:      gob.NewDecoder(&decBuf),
		listenerChan: make(chan *ReplicatorRequest, defaultListenerSize),
		replicaID:    replicaID,
	}
	go remote.startReceiver()
	return
}

func (remote *RemoteConn) listenToReplica(replicaID int64) {
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Listening to", replicaID, "msgs.")
	remote.sendCh.QueueBind(remote.recQueue.Name, strconv.FormatInt(replicaID, 10), exchangeName, false, nil)
}

func (remote *RemoteConn) SendReplicatorRequest(request *ReplicatorRequest) {
	tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending remote request:", *request)
	if len(request.Txns) > 0 {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually sending txns to other replicas.")
	}
	err := remote.encoder.Encode(request)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of request to send. Error:", err)
	}
	data := remote.encBuf.Bytes()
	//Need to make a copy as Reset() will destroy data
	copyData := make([]byte, len(data))
	copy(copyData, data)
	//tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "bytes of request to send:", copyData)
	//remote.encBuf.Reset()
	remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.SenderID, 10), false, false, amqp.Publishing{Body: copyData})
}

//This should not be called externally.
func (remote *RemoteConn) startReceiver() {
	for data := range remote.recCh {
		remote.decBuf.Write(data.Body)
		request := &ReplicatorRequest{}
		//tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "bytes of received request:", data.Body)
		err := remote.decoder.Decode(request)
		if err != nil {
			tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received request. Error:", err)
		}
		//remote.decBuf.Reset()
		tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote request:", *request)
		if len(request.Txns) > 0 {
			tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually received txns from other replicas.")
		}
		if request.SenderID == remote.replicaID {
			tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote request from myself!")
		}
		//tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "My replicaID:", remote.replicaID, "senderID:", request.SenderID)
		remote.listenerChan <- request
	}
}

func (remote *RemoteConn) GetNextRemoteRequest() (request *ReplicatorRequest) {
	return <-remote.listenerChan
}

*/
