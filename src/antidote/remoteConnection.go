package antidote

import (
	"bytes"
	"clocksi"
	"crdt"
	"encoding/gob"
	"strconv"
	"tools"

	proto "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

type RemoteConn struct {
	conn         *amqp.Connection
	sendCh       *amqp.Channel
	recQueue     amqp.Queue
	recCh        <-chan amqp.Delivery
	listenerChan chan *NewReplicatorRequest
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
		listenerChan: make(chan *NewReplicatorRequest, defaultListenerSize),
		replicaID:    replicaID,
	}
	go remote.startReceiver()
	return
}

func (remote *RemoteConn) listenToPartition(partitionID int64) {
	tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Listening to msgs for partition:", partitionID)
	remote.sendCh.QueueBind(remote.recQueue.Name, strconv.FormatInt(partitionID, 10), exchangeName, false, nil)
}

func (remote *RemoteConn) SendReplicatorRequest(request *NewReplicatorRequest) {
	tools.FancyDebugPrint(tools.REMOTE_PRINT, remote.replicaID, "Sending remote request:", *request)
	if len(request.Txns) > 0 {
		tools.FancyInfoPrint(tools.REMOTE_PRINT, remote.replicaID, "Actually sending txns to other replicas.")
	}
	protobuf := remote.createProtoReplicatePart(request)
	data, err := proto.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to generate bytes of request to send. Error:", err)
	}
	remote.sendCh.Publish(exchangeName, strconv.FormatInt(request.PartitionID, 10), false, false, amqp.Publishing{Body: data})
}

//This should not be called externally.
func (remote *RemoteConn) startReceiver() {
	for data := range remote.recCh {
		protobuf := &ProtoReplicatePart{}
		err := proto.Unmarshal(data.Body, protobuf)
		if err != nil {
			tools.FancyErrPrint(tools.REMOTE_PRINT, remote.replicaID, "Failed to decode bytes of received request. Error:", err)
		}
		request := remote.protoToReplicatorRequest(protobuf)
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

func (remote *RemoteConn) protoToReplicatorRequest(protobuf *ProtoReplicatePart) (request *NewReplicatorRequest) {
	return &NewReplicatorRequest{
		PartitionID: protobuf.GetPartitionID(),
		SenderID:    protobuf.GetSenderID(),
		StableTs:    protobuf.GetPartStableTs(),
		Txns:        remote.protoToRemoteTxns(protobuf.Txns),
	}
}

func (remote *RemoteConn) protoToRemoteTxns(protobufs []*ProtoRemoteTxns) (remoteTxns []NewRemoteTxns) {
	remoteTxns = make([]NewRemoteTxns, len(protobufs))
	for i, proto := range protobufs {
		remoteTxns[i] = NewRemoteTxns{
			Timestamp: clocksi.ClockSiTimestamp{}.FromBytes(proto.Timestamp),
			Upds:      remote.protoToDownstreamUpds(proto.Upds),
		}
	}
	return
}

func (remote *RemoteConn) protoToDownstreamUpds(protobufs []*ProtoDownstreamUpd) (upds []*UpdateObjectParams) {
	upds = make([]*UpdateObjectParams, len(protobufs))
	for i, proto := range protobufs {
		keyProto := proto.GetKeyParams()
		upd := &UpdateObjectParams{
			KeyParams: CreateKeyParams(string(keyProto.GetKey()), keyProto.GetType(), string(keyProto.GetBucket())),
		}
		upds[i] = upd
		var updArgs crdt.UpdateArguments
		switch upd.CrdtType {
		case CRDTType_COUNTER:
			updArgs = remote.protoToCounterDownstream(proto.GetCounterOp())
		case CRDTType_ORSET:
			updArgs = remote.protoToSetDownstream(proto.GetSetOp())
		}
		upd.UpdateArgs = updArgs
	}
	return upds
}

func (remote *RemoteConn) protoToCounterDownstream(protobuf *ProtoCounterDownstream) (args crdt.UpdateArguments) {
	if protobuf.GetIsInc() {
		return crdt.Increment{Change: protobuf.GetChange()}
	} else {
		return crdt.Decrement{Change: protobuf.GetChange()}
	}
}

func (remote *RemoteConn) protoToSetDownstream(protobuf *ProtoSetDownstream) (args crdt.UpdateArguments) {
	//TODO: Test if this works, as maybe it is set as an empty slice?
	if adds := protobuf.GetAdds(); adds != nil {
		elems := make(map[crdt.Element]crdt.Unique)
		for _, pairProto := range adds {
			elems[crdt.Element(pairProto.GetValue())] = crdt.Unique(pairProto.GetUnique())
		}
		return crdt.DownstreamAddAll{Elems: elems}
	} else {
		elems := make(map[crdt.Element]crdt.UniqueSet)
		for _, pairProto := range protobuf.GetRems() {
			elems[crdt.Element(pairProto.GetValue())] = crdt.UInt64ArrayToUniqueSet(pairProto.GetUniques())
		}
		return crdt.DownstreamRemoveAll{}
	}
}

func (remote *RemoteConn) GetNextRemoteRequest() (request *NewReplicatorRequest) {
	return <-remote.listenerChan
}

//TODO: These probably should be moved to protoLib.
func (remote *RemoteConn) createProtoReplicatePart(request *NewReplicatorRequest) (protobuf *ProtoReplicatePart) {
	return &ProtoReplicatePart{
		SenderID:     &request.SenderID,
		PartitionID:  &request.PartitionID,
		PartStableTs: &request.StableTs,
		Txns:         remote.createProtoRemoteTxns(request.Txns),
	}
}

func (remote *RemoteConn) createProtoRemoteTxns(txnsRequests []NewRemoteTxns) (protobufs []*ProtoRemoteTxns) {
	protobufs = make([]*ProtoRemoteTxns, len(txnsRequests))
	for i, req := range txnsRequests {
		protobufs[i] = &ProtoRemoteTxns{
			Timestamp: req.Timestamp.ToBytes(),
			Upds:      remote.createProtoDownstreamUpds(&req),
		}
	}
	return protobufs
}

func (remote *RemoteConn) createProtoDownstreamUpds(req *NewRemoteTxns) (protobufs []*ProtoDownstreamUpd) {
	protobufs = make([]*ProtoDownstreamUpd, len(req.Upds))
	for i, upd := range req.Upds {
		protobufs[i] = &ProtoDownstreamUpd{KeyParams: createBoundObject(upd.Key, upd.CrdtType, upd.Bucket)}
		switch upd.CrdtType {
		case CRDTType_COUNTER:
			protobufs[i].CounterOp = remote.createProtoCounterDownstream(&upd.UpdateArgs)
		case CRDTType_ORSET:
			protobufs[i].SetOp = remote.createProtoSetDownstream(&upd.UpdateArgs)
		}
	}
	return protobufs
}

func (remote *RemoteConn) createProtoCounterDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoCounterDownstream) {
	switch convertedUpd := (*upd).(type) {
	case crdt.Increment:
		return &ProtoCounterDownstream{IsInc: proto.Bool(true), Change: &convertedUpd.Change}
	case crdt.Decrement:
		return &ProtoCounterDownstream{IsInc: proto.Bool(false), Change: &convertedUpd.Change}
	}
	return nil
}

func (remote *RemoteConn) createProtoSetDownstream(upd *crdt.UpdateArguments) (protobuf *ProtoSetDownstream) {
	switch convertedUpd := (*upd).(type) {
	case crdt.DownstreamAddAll:
		addProto := &ProtoSetDownstream{Adds: make([]*ProtoValueUnique, len(convertedUpd.Elems))}
		i := 0
		for value, unique := range convertedUpd.Elems {
			intUnique := uint64(unique)
			addProto.Adds[i] = &ProtoValueUnique{Value: []byte(value), Unique: &intUnique}
			i++
		}
		protobuf = addProto
	case crdt.DownstreamRemoveAll:
		remProto := &ProtoSetDownstream{Rems: make([]*ProtoValueUniques, len(convertedUpd.Elems))}
		i := 0
		for value, uniques := range convertedUpd.Elems {
			//TODO: Probably pass this uniquesInt thing to commonTools
			uniquesInts := make([]uint64, len(uniques))
			j := 0
			for unique := range uniques {
				uniquesInts[j] = uint64(unique)
				j++
			}
			remProto.Rems[i] = &ProtoValueUniques{Value: []byte(value), Uniques: uniquesInts}
			i++
		}
		protobuf = remProto
	}
	return
}

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
