package antidote

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
	err = sendCh.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil)
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
	tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Added interest on", replicaID, "msgs.")
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
