package antidote

import (
	"clocksi"
	"crdt"
	"encoding/gob"
	"reflect"
	"strconv"
	"tools"

	proto "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

type RemoteConn struct {
	conn         *amqp.Connection
	sendCh       *amqp.Channel
	recCh        <-chan amqp.Delivery
	listenerChan chan ReplicatorMsg
	replicaID    int64
}

const (
	protocol            = "amqp://"
	ip                  = "guest:guest@localhost:"
	port                = "5672/"
	exchangeName        = "objRepl"
	exchangeType        = "topic"
	defaultListenerSize = 100
	clockTopic          = "clk"
)

//Topics (i.e., queue filters): partitionID.bucket
//There's one queue per replica. Each replica's queue will receive messages from ALL other replicas, as long as the topics match the ones
//that were binded to the replica's queue.

//TODO: Remove replicaID, it's just here for debugging purposes
func CreateRemoteConnStruct(partitionsToListen []uint64, replicaID int64) (remote *RemoteConn, err error) {
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
	//The previously declared queue will receive messages from any replica that publishes for the partitions in partitionsToListen
	for _, partId := range partitionsToListen {
		sendCh.QueueBind(queue.Name, strconv.FormatUint(partId, 10)+".*", exchangeName, false, nil)
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

	//initializing gob. Need to register all types which will be used as interface implementations
	gob.Register(clocksi.DummyTs)
	for _, crdt := range crdt.DummyCRDTs {
		args := crdt.GetPossibleDownstreamTypes()
		for _, arg := range args {
			gob.Register(arg)
		}
	}

	remote = &RemoteConn{
		sendCh:       sendCh,
		conn:         conn,
		recCh:        recCh,
		listenerChan: make(chan ReplicatorMsg, defaultListenerSize),
		replicaID:    replicaID,
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
	protobuf := createProtoReplicatePart(request)
	data, err := proto.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of partTxn request to send. Error:", err)
	}
	//TODO: Actually I need to separate this further into buckets (i.e., a msg per bucket). Sigh.
	remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.PartitionID, 10)+".*", false, false, amqp.Publishing{Body: data})
}

func (remote *RemoteConn) SendStableClk(ts int64) {
	tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending stable clk:", ts)
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
		//This code duplication can be avoided once I remove these messages
		if data.RoutingKey == clockTopic {
			protobuf := &ProtoStableClock{}
			err := proto.Unmarshal(data.Body, protobuf)
			if err != nil {
				tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received stableClock. Error:", err)
			}
			request := protoToStableClock(protobuf)
			//remote.decBuf.Reset()
			tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote stableClock:", *request)
			//TODO: Avoid receiving own messages?
			if request.SenderID == remote.replicaID {
				tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Ignored received stableClock as it was sent by myself.")
			} else {
				remote.listenerChan <- request
			}
		} else {
			//TODO: Maybe analyze data.routingKey to avoid decoding the protobuf if it was sent by this own replica?
			protobuf := &ProtoReplicatePart{}
			err := proto.Unmarshal(data.Body, protobuf)
			if err != nil {
				tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received request. Error:", err)
			}
			request := protoToReplicatorRequest(protobuf)
			//remote.decBuf.Reset()
			tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Received remote request:", *request)
			if len(request.Upds) > 0 && request.SenderID != remote.replicaID {
				tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually received txns from other replicas.")
				for _, upd := range request.Upds {
					tools.FancyWarnPrint(tools.REMOTE_PRINT, remote.replicaID, "Received downstream args:", upd, "type:", reflect.TypeOf(upd.UpdateArgs))
				}
			}
			if request.SenderID != remote.replicaID {
				remote.listenerChan <- request
			} else {
				tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Ignored request from self.")
			}
			//tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "My replicaID:", remote.replicaID, "senderID:", request.SenderID)
		}
	}
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
