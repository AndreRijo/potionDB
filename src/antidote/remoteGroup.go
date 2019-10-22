package antidote

import (
	fmt "fmt"
	"strings"
	"tools"
)

//Handles multiple remoteConnections.go. Abstracts multiple RabbitMQ instances as if it was one only.
//Later on this will probably also be responsible for finding other replicas/datacenters.

type RemoteGroup struct {
	ourConn   *RemoteConn        //connection to this replica's/datacenter's RabbitMQ instance
	conns     []*RemoteConn      //groups to listen msgs from (includes ourConn)
	groupChan chan ReplicatorMsg //Groups requests sent by each remoteConnection.
}

const (
	defaultListenerSize = 100
)

//docker run -d --hostname RMQ1 --name rabbitmq1 -p 5672:5672 rabbitmq:latest

//func CreateRemoteGroupStruct(myInstanceIP string, othersIPList []string, bucketsToListen []string, replicaID int64) (group *RemoteGroup, err error) {
func CreateRemoteGroupStruct(bucketsToListen []string, replicaID int16) (group *RemoteGroup, err error) {
	myInstanceIP := tools.SharedConfig.GetConfig("localRabbitMQAddress")
	othersIPList := strings.Split(tools.SharedConfig.GetConfig("remoteRabbitMQAddresses"), " ")

	group = &RemoteGroup{conns: make([]*RemoteConn, len(othersIPList)+1), groupChan: make(chan ReplicatorMsg, defaultListenerSize*len(othersIPList)+1)}
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
}

func (group *RemoteGroup) SendPartTxn(request *NewReplicatorRequest) {
	/*
		for _, conn := range group.conns {
			conn.SendPartTxn(request)
		}
	*/
	group.ourConn.SendPartTxn(request)
}

func (group *RemoteGroup) SendStableClk(ts int64) {
	/*
		for _, conn := range group.conns {
			conn.SendStableClk(ts)
		}
	*/
	group.ourConn.SendStableClk(ts)
}

func (group *RemoteGroup) GetNextRemoteRequest() (request ReplicatorMsg) {
	return <-group.groupChan
}

func (group *RemoteGroup) prepareMsgListener() {
	for i := range group.conns {
		go group.listenToRemoteConn(group.conns[i].listenerChan)
	}
}

func (group *RemoteGroup) listenToRemoteConn(channel chan ReplicatorMsg) {
	for msg := range channel {
		group.groupChan <- msg
	}
}
