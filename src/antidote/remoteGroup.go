package antidote

import (
	fmt "fmt"
	"potionDB/src/clocksi"
	"potionDB/src/tools"
	"strings"

	pb "github.com/golang/protobuf/proto"
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
}

type GroupOrErr struct {
	*RemoteConn
	error
	index int
}

const (
	defaultListenerSize = 100
)

//docker run -d --hostname RMQ1 --name rabbitmq1 -p 5672:5672 rabbitmq:latest

func CreateRemoteGroupStruct(bucketsToListen []string, replicaID int16) (group *RemoteGroup, err error) {
	myInstanceIP := tools.SharedConfig.GetOrDefault("localRabbitMQAddress", "localhost:5672")
	othersIPList := strings.Split(tools.SharedConfig.GetConfig("remoteRabbitMQAddresses"), " ")
	if len(othersIPList) == 1 && len(othersIPList[0]) < 2 {
		othersIPList = []string{}
	}

	group = &RemoteGroup{conns: make([]*RemoteConn, len(othersIPList)), nReplicas: int16(len(othersIPList)),
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
		group.ourConn, err = CreateRemoteConnStruct(myInstanceIP, bucketsToListen, replicaID)
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
		go connectToIp(ip, i, bucketsToListen, replicaID, int16(i), openConnsChan, false)
		group.knownIPs[ip] = int16(i)
	}
	selfConnChan := make(chan GroupOrErr)
	fmt.Println(myInstanceIP)
	copy := myInstanceIP
	go connectToIp(copy, -1, bucketsToListen, replicaID, int16(len(othersIPList)), selfConnChan, true)

	//Wait for self first
	reply := <-selfConnChan
	if reply.error != nil {
		fmt.Printf("Error while connecting to this replica's RabbitMQ at %s: %v", myInstanceIP, err)
		panic(nil)
	}
	group.ourConn = reply.RemoteConn
	//group.conns[len(othersIPList)] = reply.RemoteConn
	fmt.Println("Connected to self RabbitMQ instance at", myInstanceIP)

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
	fmt.Println("Number of conns:", len(group.conns))
	group.prepareMsgListener()
	return
}

func connectToIp(ip string, index int, bucketsToListen []string, replicaID int16, connID int16, connChan chan GroupOrErr, isSelfConn bool) {
	reply := GroupOrErr{index: index}
	reply.RemoteConn, reply.error = CreateRemoteConnStruct(ip, bucketsToListen, replicaID, connID, isSelfConn)
	connChan <- reply
}

//Adds a replica if it isn't already known - a joining replica might be already known e.g. when two new replicas start at the same time, aware of each other.
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

	go connectToIp(ip, int(slot), bucketsToListen, group.replicaID, slot, connChan, false)
	reply := <-connChan
	group.conns[slot] = reply.RemoteConn //Update this connection index
	group.knownIPs[ip] = slot
	fmt.Println("Finish add replica, nReplicas:", group.nReplicas)
	go group.listenToRemoteConn(reply.RemoteConn.listenerChan)
	return slot
}

func (group *RemoteGroup) SendPartTxn(request *NewReplicatorRequest) {
	group.ourConn.SendPartTxn(request)
}

func (group *RemoteGroup) SendStableClk(ts int64) {
	group.ourConn.SendStableClk(ts)
}

func (group *RemoteGroup) SendFullStableClk(clk clocksi.Timestamp) {
	group.ourConn.SendFullStableClk(clk)
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

func (group *RemoteGroup) sendReplicaID() {
	//Same msg for everyone, so we prepare it here
	protobuf := createProtoRemoteID(group.replicaID)
	data, err := pb.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, group.replicaID, "Failed to generate bytes of RemoteID msg:", err)
	}
	for _, conn := range group.conns {
		conn.SendRemoteID(data)
	}
}

func (group *RemoteGroup) SendJoin(buckets []string, replicaID int16) {
	//Same msg for everyone, so we prepare it here
	protobuf := createProtoJoin(buckets, replicaID, tools.SharedConfig.GetConfig("localRabbitMQAddress"))
	data, err := pb.Marshal(protobuf)
	if err != nil {
		tools.FancyErrPrint(tools.REMOTE_PRINT, group.replicaID, "Failed to generate bytes of Join msg:", err)
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
