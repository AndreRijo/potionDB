package main

import (
	"antidote"
	"bufio"
	"crdt"
	"fmt"
	rand "math/rand"
	"net"
	"os"
	"time"
	"tools"

	proto "github.com/golang/protobuf/proto"
)

const (
	bucket      = "bkt"
	maxId       = 50000000
	maxValue    = 50
	targetTrans = 50
	writeProb   = 0.7
	//writeProb = 0.5
	minOpsPerTrans   = 3
	maxOpsPerTrans   = 10
	maxSleepTime     = 100
	nClients         = 3
	beforeStartSleep = 2000
)

var (
	//keys   = [2]string{"topk1", "topk2"}
	keys = [4]string{"counter1", "counter2", "counter3", "counter4"}
	//buckets = [2][]byte{[]byte("bkt1"), []byte("bkt2")}
	buckets = [2]string{"bkt1", "bkt2"}
	reader  = bufio.NewReader(os.Stdin)
)

/*
Plan: execute "x" transactions, each one doing a certain number of updates and reads (maybe use chance?).
Give some random (but low) delay between transactions. Repeat if they get aborted.
*/

func ClientMain() {
	connection, err := net.Dial("tcp", "127.0.0.1:8087")
	tools.CheckErr("Network connection establishment err", err)
	rand.Seed(time.Now().UTC().UnixNano())
	/*
		for i := 0; i < nClients; i++ {
			go transactionCycle(connection)
		}
		fmt.Println("Click enter once transactions stop happening.")
		reader.ReadString('\n')
	*/
	testStaticUpdate(connection)
	time.Sleep(time.Duration(1000) * time.Millisecond)
	testStaticRead(connection)
}

//TODO: Test this and testStaticUpdate() with antidote
func testStaticRead(connection net.Conn) {
	reads := make([]antidote.ReadObjectParams, 10)
	for i := 0; i < len(reads); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		reads[i] = antidote.ReadObjectParams{
			KeyParams: antidote.CreateKeyParams(rndKey, antidote.CRDTType_COUNTER, rndBucket),
		}
	}

	proto := antidote.CreateStaticReadObjs(reads)
	antidote.SendProto(antidote.StaticReadObjs, proto, connection)
	fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	_, receivedProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Received proto: ", receivedProto)
}

func testStaticUpdate(connection net.Conn) {
	updates := make([]antidote.UpdateObjectParams, 1)
	inc := rand.Int31n(100)
	for i := 0; i < len(updates); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		fmt.Println("Incrementing with: ", inc)
		updates[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.CreateKeyParams(rndKey, antidote.CRDTType_COUNTER, rndBucket),
			UpdateArgs: crdt.Increment{Change: inc},
		}
	}

	proto := antidote.CreateStaticUpdateObjs(updates)
	antidote.SendProto(antidote.StaticUpdateObjs, proto, connection)
	fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Received type, proto: ", protoType, receivedProto)
}

func transactionCycle(connection net.Conn) {
	fmt.Println("Sleeping a bit before starting...")
	time.Sleep(time.Duration(beforeStartSleep) * time.Millisecond)

	for nDone := 0; nDone < targetTrans; nDone++ {
		fmt.Println("Starting transaction...")
		//Send start transaction
		startTrans := antidote.CreateStartTransaction()
		//reader.ReadString('\n')
		antidote.SendProto(antidote.StartTrans, startTrans, connection)

		//Receive transaction ID
		_, receivedProto, _ := antidote.ReceiveProto(connection)
		startTransResp := receivedProto.(*antidote.ApbStartTransactionResp)
		transId := startTransResp.GetTransactionDescriptor()
		//fmt.Println("transId:", transId, "len:", len(transId))
		//fmt.Println("error code:", startTransResp.GetErrorcode())
		//fmt.Println("success?", startTransResp.GetSuccess())

		fmt.Println("Starting to send operations...")
		createAndSendOps(connection, transId)
		//debugWithTopk(connection, transId)
		//debugWithCounter(connection, transId)

		fmt.Println("Sending commit...")
		commitTrans := antidote.CreateCommitTransaction(transId)
		//fmt.Println(commitTrans)
		//reader.ReadString('\n')
		antidote.SendProto(antidote.CommitTrans, commitTrans, connection)

		//Receive reply, check if it is commit or abort?
		_, receivedProto, _ = antidote.ReceiveProto(connection)
		commitReply := receivedProto.(*antidote.ApbCommitResp)
		if !commitReply.GetSuccess() {
			fmt.Println("Commit failed, retrying...")
			nDone--
			fmt.Println("(actually, for now, we're actually exiting...")
			os.Exit(1)
		} else {
			fmt.Println("Commit", nDone, "out of", targetTrans, "done.")
		}

		//Always sleep a bit
		toSleep := rand.Intn(maxSleepTime)
		fmt.Println("Sleeping for", toSleep, "ms")
		time.Sleep(time.Duration(toSleep) * time.Millisecond)
	}

	fmt.Println("Finish!")
}

func debugWithCounter(connection net.Conn, transId []byte) {
	counterKeys := [3]string{"counter1", "counter2", "counter3"}
	protoType, writeProto := antidote.UpdateObjs, createCounterWrite(transId, counterKeys[0])
	fmt.Println("Sending write for", counterKeys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), writeProto, connection)
	replyType, replyProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType, replyProto)

	protoType, readProto := antidote.ReadObjs, createRead(transId, counterKeys[0], antidote.CRDTType_COUNTER)
	fmt.Println("Sending read for", counterKeys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), readProto, connection)
	replyType1, replyProto1, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType1, replyProto1)

	protoType, readProto = antidote.ReadObjs, createRead(transId, counterKeys[1], antidote.CRDTType_COUNTER)
	fmt.Println("Sending read for", counterKeys[1])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), readProto, connection)
	replyType2, replyProto2, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType2, replyProto2)
	//reader.ReadString('\n')
}

func debugWithTopk(connection net.Conn, transId []byte) {

	protoType, writeProto := antidote.UpdateObjs, getNextWrite(transId, keys[0])
	fmt.Println("Sending write for", keys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), writeProto, connection)
	replyType, replyProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType, replyProto)

	/*
		protoType, readProto := antidote.ReadObjs, getNextRead(transId, keys[1])
		fmt.Println("Sending read for", keys[1])
		//reader.ReadString('\n')
		antidote.SendProto(byte(protoType), readProto, connection)
		replyType1, replyProto1, _ := antidote.ReceiveProto(connection)
		fmt.Println("Reply type, proto:", replyType1, replyProto1)
	*/

	protoType, readProto := antidote.ReadObjs, getNextRead(transId, keys[0])
	fmt.Println("Sending read for", keys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), readProto, connection)
	replyType2, replyProto2, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType2, replyProto2)
	//reader.ReadString('\n')

	/*
		fmt.Println("Sending commit")
		commitTrans := antidote.CreateCommitTransaction(transId)
		antidote.SendProto(antidote.CommitTrans, commitTrans, connection)
		fmt.Println("Commit sent.")
		_, receivedProto, _ := antidote.ReceiveProto(connection)
		commitReply := receivedProto.(*antidote.ApbCommitResp)
		fmt.Println("Commit success:", commitReply.GetSuccess())
	*/
}

func createAndSendOps(connection net.Conn, transId []byte) {
	nOps := rand.Intn(maxOpsPerTrans-minOpsPerTrans+1) + minOpsPerTrans
	for currOpN := 0; currOpN < nOps; currOpN++ {
		//fmt.Println("Step", currOpN, "out of", nOps)
		protoType, opProto := getNextOp(transId)
		//fmt.Println("Generated proto", opProto)
		//fmt.Println("Sending proto op")
		//reader.ReadString('\n')
		antidote.SendProto(protoType, opProto, connection)
		//fmt.Println("Receiving proto op")
		//_, replyProto, _ := antidote.ReceiveProto(connection)
		replyType, _, _ := antidote.ReceiveProto(connection)
		if replyType == antidote.ErrorReply {
			fmt.Println("Received ApbErrorResp!")
			os.Exit(1)
		}
		//fmt.Println("Received reply op proto", replyProto)
		//For now, ignore.
	}
	//fmt.Println("nOps done")
}

func getNextOp(transId []byte) (protoType byte, protoBuf proto.Message) {
	key := keys[rand.Intn(len(keys))]
	if rand.Float32() < writeProb {
		//fmt.Println("Next proto is a write.")
		protoType, protoBuf = antidote.UpdateObjs, getNextWrite(transId, key)
	} else {
		//fmt.Println("Next proto is a read.")
		protoType, protoBuf = antidote.ReadObjs, getNextRead(transId, key)
	}
	if protoBuf == nil {
		fmt.Println("Warning - nil protoBuf on getNextOp!")
	}
	return
}

func getNextWrite(transId []byte, key string) (updateBuf *antidote.ApbUpdateObjects) {
	//fmt.Println("Creating write")
	rndPlayer, rndValue := rand.Intn(maxId), rand.Intn(maxValue)
	//rndPlayer, rndValue := 1, 887
	topkWrite := antidote.CreateTopkUpdate(rndPlayer, rndValue)
	updateBuf = antidote.CreateUpdateObjs(transId, key, antidote.CRDTType_TOPK, bucket, topkWrite)
	//updateBuf = antidote.CreateUpdateObjs(transId, "topk1", antidote.CRDTType_TOPK, "bkt", topkWrite)
	//fmt.Println("Write transId:", updateBuf.GetTransactionDescriptor())
	//fmt.Println("Values sent:", rndPlayer, rndValue)
	return
}

func getNextRead(transId []byte, key string) (readBuf *antidote.ApbReadObjects) {
	//fmt.Println("Creating read")
	readBuf = antidote.CreateReadObjs(transId, key, antidote.CRDTType_TOPK, bucket)
	//fmt.Println("Read transId:", readBuf.GetTransactionDescriptor())
	return
}

func createRead(transId []byte, key string, crdtType antidote.CRDTType) (readBuf *antidote.ApbReadObjects) {
	//fmt.Println("Creating read")
	readBuf = antidote.CreateReadObjs(transId, key, crdtType, bucket)
	//fmt.Println("Read transId:", readBuf.GetTransactionDescriptor())
	return
}

func createCounterWrite(transId []byte, key string) (updateBuf *antidote.ApbUpdateObjects) {
	//fmt.Println("Creating write")
	write := antidote.CreateCounterUpdate(5)
	updateBuf = antidote.CreateUpdateObjs(transId, key, antidote.CRDTType_COUNTER, bucket, write)
	//fmt.Println("Write transId:", updateBuf.GetTransactionDescriptor())
	return
}

func getRandomLocationParams() (key string, bucket string) {
	key, bucket = keys[rand.Intn(len(keys))], buckets[rand.Intn(len(buckets))]
	return
}
