package components

import (
	fmt "fmt"
	"os"

	"potionDB/crdt/crdt"
	"potionDB/potionDB/utilities"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

//Handles multiple remoteConnections.go. Abstracts multiple RabbitMQ instances as if it was one only.
//Later on this will probably also be responsible for finding other replicas/datacenters.

type RemoteGroup struct {
	ourConn   *RemoteConn        //connection to this replica's/datacenter's RabbitMQ instance
	conns     []*RemoteConn      //groups to listen msgs from (includes ourConn)
	groupChan chan ReplicatorMsg //Groups requests sent by each remoteConnection.
	replicaID int16              //For msgs purposes
	nReplicas int16              //This counter is incremented as soon as a connection is attempted to be established. Helps with uniquely identifying the connections.
	knownIPs  map[string]int16   //Used to verify if a received join corresponds to an already known replica or not. Stores the position in conns of each replica.
	workChan  chan RCWork
}

type GroupOrErr struct {
	*RemoteConn
	error
	index int
}

type RCWork interface {
	DoWork(replicaID int16)
}

type MarshallWork struct {
	/*Bucket string
	Upds   map[int][]crdt.UpdateObjectParams
	ReplyChan */
	BucketOps map[string]map[int][]crdt.UpdateObjectParams
	Txn       RemoteTxn
	ReplyChan chan MarshallWorkReply
}

type MarshallWorkReply struct {
	Result    []PairKeyBytes
	TxnSendId int32
}

type GroupMarshallWork struct {
	Bucket    string
	Txns      []RemoteTxn
	BktTxns   []RemoteTxn
	ReplyChan chan PairKeyBytes //Actually stores bucket, data
}

const (
	defaultListenerSize = 100
)

var othersIPList []string

//docker run -d --hostname RMQ1 --name rabbitmq1 -p 5672:5672 rabbitmq:latest

func CreateRemoteGroupStruct(bucketsToListen []string, replicaID int16) (group *RemoteGroup, err error) {
	//myInstanceIP := tools.SharedConfig.GetOrDefault("localRabbitMQAddress", "localhost:5672")
	//othersIPList := strings.Split(tools.SharedConfig.GetConfig("remoteRabbitMQAddresses"), " ")
	if len(othersIPList) == 1 && len(othersIPList[0]) < 2 {
		othersIPList = []string{}
	}
	fmt.Println("[RG]Remote conns:", othersIPList, "(size:", len(othersIPList), ")")

	group = &RemoteGroup{conns: make([]*RemoteConn, len(othersIPList)), nReplicas: int16(len(othersIPList)), workChan: make(chan RCWork, 100),
		groupChan: make(chan ReplicatorMsg, defaultListenerSize*len(othersIPList)), replicaID: replicaID, knownIPs: make(map[string]int16)}

	/*
		for i, ip := range othersIPList {
			fmt.Println("Connecting to", ip)
			group.conns[i], err = CreateRemoteConnStruct(ip, bucketsToListen, replicaID)
			if err != nil {
				fmt.Println("Error while connecting to RabbitMQ:", err)
				return nil, err
			}
			fmt.Println("Connected to", ip)
		}
		group.ourConn, err = CreateRemoteConnStruct(localRabbitMQIP, bucketsToListen, replicaID)
		if err != nil {
			return nil, err
		}
		group.conns[len(othersIPList)] = group.ourConn
		group.prepareMsgListener()
		return
	*/
	openConnsChan := make(chan GroupOrErr, 10)
	for i, ip := range othersIPList {
		fmt.Println(i, ip)
		go connectToIp(ip, i, bucketsToListen, replicaID, int16(i), openConnsChan, false, group.workChan)
		group.knownIPs[ip] = int16(i)
	}
	selfConnChan := make(chan GroupOrErr)
	fmt.Println(localRabbitMQIP)
	copy := localRabbitMQIP
	go connectToIp(copy, -1, bucketsToListen, replicaID, int16(len(othersIPList)), selfConnChan, true, group.workChan)

	//Wait for self first
	reply := <-selfConnChan
	if reply.error != nil {
		fmt.Printf("Error while connecting to this replica's RabbitMQ at %s: %v", localRabbitMQIP, err)
		panic("")
	}
	group.ourConn = reply.RemoteConn
	//group.conns[len(othersIPList)] = reply.RemoteConn
	fmt.Println("Connected to self RabbitMQ instance at", localRabbitMQIP)

	//Wait for others
	for i := 0; i < len(othersIPList); i++ {
		reply := <-openConnsChan
		if reply.error != nil {
			fmt.Printf("Error while connecting to remote RabbitMQ at %s: %v", othersIPList[reply.index], err)
			return nil, err
		}
		group.conns[i] = reply.RemoteConn
		fmt.Println("Connected to", othersIPList[reply.index])
	}
	fmt.Println("All RabbitMQ connections established. Number of conns (not counting self):", len(group.conns))
	group.prepareMsgListener()
	group.prepareWorkerRoutines()
	return
}

func connectToIp(ip string, index int, bucketsToListen []string, replicaID int16, connID int16, connChan chan GroupOrErr, isSelfConn bool, workChan chan RCWork) {
	reply := GroupOrErr{index: index}
	reply.RemoteConn, reply.error = CreateRemoteConnStruct(ip, bucketsToListen, replicaID, connID, isSelfConn, workChan)
	connChan <- reply
}

// Adds a replica if it isn't already known - a joining replica might be already known e.g. when two new replicas start at the same time, aware of each other.
func (group *RemoteGroup) AddReplica(ip string, bucketsToListen []string, joiningReplicaID int16) (connID int16) {
	if id, has := group.knownIPs[ip]; has {
		fmt.Println("Didn't add replica as it is already known.", ip)
		//Already known replica, nothing to do
		return id
	}
	fmt.Println("Start add replica, nReplicas:", group.nReplicas)
	connChan := make(chan GroupOrErr)
	slot := group.nReplicas
	group.nReplicas += 1
	group.conns = append(group.conns, nil) //Fill "slot" for this connection

	go connectToIp(ip, int(slot), bucketsToListen, group.replicaID, slot, connChan, false, group.workChan)
	reply := <-connChan
	group.conns[slot] = reply.RemoteConn //Update this connection index
	group.knownIPs[ip] = slot
	fmt.Println("Finish add replica, nReplicas:", group.nReplicas)
	go group.listenToRemoteConn(reply.RemoteConn.listenerChan)
	return slot
}

/*
func (group *RemoteGroup) SendPartTxn(request *NewReplicatorRequest) {
	group.ourConn.SendPartTxn(request)
}
*/

func (group *RemoteGroup) SendGroupTxn(txns []RemoteTxn) {
	group.ourConn.SendGroupTxn(txns)
}

/*func (group *RemoteGroup) SendTxn(txn RemoteTxn) {
	group.ourConn.SendTxn(txn)
}*/

func (group *RemoteGroup) SendTxnsIndividually(txns []RemoteTxn) {
	group.ourConn.SendTxnsIndividually(txns)
}

func (group *RemoteGroup) SendStableClk(ts int64) {
	group.ourConn.SendStableClk(ts)
}

func (group *RemoteGroup) SendTrigger(trigger AutoUpdate, isGeneric bool) {
	group.ourConn.SendTrigger(trigger, isGeneric)
}

func (group *RemoteGroup) GetNextRemoteRequest() (request ReplicatorMsg) {
	return <-group.groupChan
}

func (group *RemoteGroup) prepareMsgListener() {
	for i := range group.conns {
		go group.listenToRemoteConn(group.conns[i].listenerChan)
	}
	go group.listenToRemoteConn(group.ourConn.listenerChan)
}

func (group *RemoteGroup) listenToRemoteConn(channel chan ReplicatorMsg) {
	for msg := range channel {
		group.groupChan <- msg
	}
}

func (group *RemoteGroup) sendReplicaID(buckets []string, ip string) {
	//Same msg for everyone, so we prepare it here
	protobuf := createProtoRemoteID(group.replicaID, buckets, ip)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, group.replicaID, "Failed to generate bytes of RemoteID msg:", err)
	}
	for _, conn := range group.conns {
		conn.SendRemoteID(data)
	}
}

func (group *RemoteGroup) SendJoin(buckets []string, replicaID int16) {
	//Same msg for everyone, so we prepare it here
	protobuf := createProtoJoin(buckets, replicaID, localRabbitMQIP)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		utilities.FancyErrPrint(utilities.REMOTE_PRINT, group.replicaID, "Failed to generate bytes of Join msg:", err)
	}
	fmt.Println("Sending joins as", replicaID, "to", len(group.conns), "replicas")
	for _, conn := range group.conns {
		conn.SendJoin(data)
	}
}

func (group *RemoteGroup) SendReplyJoin(req ReplyJoin, replicaTo int16) {
	group.conns[replicaTo].SendReplyJoin(req)
}

func (group *RemoteGroup) SendRequestBucket(req RequestBucket, replicaToIP string) {
	group.conns[group.knownIPs[replicaToIP]].SendRequestBucket(req)
}

func (group *RemoteGroup) SendReplyBucket(req ReplyBucket, replicaToIP string) {
	fmt.Println("Sending ReplyBucket to", replicaToIP)
	group.conns[group.knownIPs[replicaToIP]].SendReplyBucket(req)
}

func (group *RemoteGroup) SendReplyEmpty(replicaTo int16) {
	group.conns[replicaTo].SendReplyEmpty()
}

func (group *RemoteGroup) prepareWorkerRoutines() {
	for i := 0; i < minTxnsToGroup; i++ {
		go group.listenForWork()
	}
}

func (group *RemoteGroup) listenForWork() {
	var work RCWork
	for {
		work = <-group.workChan
		work.DoWork(group.replicaID)
	}
}

func (work MarshallWork) DoWork(replicaID int16) {
	//start := time.Now()
	results, i := make([]PairKeyBytes, len(work.BucketOps)), 0
	for bucket, upds := range work.BucketOps {
		protobuf := createProtoReplicateTxn(replicaID, work.Txn.Clk, upds, work.Txn.TxnID)
		//fmt.Printf("[RC][SendTxn]Finished creating protoReplicateTxn. TxnClk: %s. Started at: %s. Current time: %s.\n",
		//work.Txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), time.Now().Format("2006-01-02 15:04:05.000"))
		data, err := pb.Marshal(protobuf)
		//fmt.Printf("[RC][SendTxn]Finished marshalling protoReplicateTxn. TxnClk: %s. Started at: %s. Current time: %s.\n",
		//work.Txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), time.Now().Format("2006-01-02 15:04:05.000"))
		if err != nil {
			//remote.checkProtoError(err, protobuf, upds)
			//fmt.Printf("[RC]Error creating ProtoReplicateTxn (error: %v). Proto: %v. Upds: %v. Timestamp: %s.\n", err, protobuf, upds,
			//(clocksi.ClockSiTimestamp{}.FromBytes(protobuf.GetTimestamp())).ToSortedString())
			os.Exit(0)
		}
		results[i] = PairKeyBytes{Key: bucketTopicPrefix + bucket, Data: data}
		i++
		//end := time.Now()
		//fmt.Printf("[RC][SendTxn]Finished sending protoReplicateTxn. TxnClk: %s. Started at: %s. End time: %s. Total time taken: %d\n",
		//work.Txn.Clk.ToSortedString(), start.Format("2006-01-02 15:04:05.000"), end.Format("2006-01-02 15:04:05.000"), (end.UnixNano()-start.UnixNano())/1000000)
	}
	work.ReplyChan <- MarshallWorkReply{Result: results, TxnSendId: work.Txn.TxnID}
}

func (work GroupMarshallWork) DoWork(replicaID int16) {
	protobuf := createProtoReplicateGroupTxn(replicaID, work.Txns, work.BktTxns)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		fmt.Printf("[RC]Error marshalling ProtoReplicateGroupTxn proto. Protobuf: %+v\n", protobuf)
		os.Exit(0)
	}
	work.ReplyChan <- PairKeyBytes{Key: work.Bucket, Data: data}
}
