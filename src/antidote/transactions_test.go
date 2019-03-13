package antidote

import (
	"clocksi"
	"crdt"
	fmt "fmt"
	"math/rand"
	"testing"
	"time"
)

const (
	initializeTime = 200 //Time to wait for the materializer to finish initializing.
	//Increase this if tests are failing due to all goroutines going to sleep.

	static    = true
	nonStatic = false
)

type testUpdateReply struct {
	updateReply       TMUpdateReply
	staticUpdateReply TMStaticUpdateReply
}

type testReadReply struct {
	readReply       []crdt.State
	staticReadReply TMStaticReadReply
}

//TODO: Lots of common code between different tests... Maybe find common code and extract to one or more methods?

/*****TESTS*****/

/*
Goal: do 2 writes in different partitions, with the 2nd write using the clock returned by
the 1st write.
Success: if both operations commit and a read returns the values written.
*/
func TestWrites1(t *testing.T) {
	tm := Initialize(0)

	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)
	firstKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")
	secondKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")

	//We want to force the two keys to go to different partitions
	for GetChannelKey(secondKey) == GetChannelKey(firstKey) {
		secondKey.Key = string(fmt.Sprint(rand.Uint64()))
	}

	firstWriteParams := createRandomSetAdd(firstKey)
	firstWriteReq, firstWriteChan := createStaticWrite(TransactionId(rand.Uint64()), clocksi.NewClockSiTimestamp(), firstWriteParams)

	go tm.handleStaticTMUpdate(firstWriteReq)
	firstWriteReply := <-firstWriteChan

	if firstWriteReply.Err != nil {
		t.Error("Error on first write: ", firstWriteReply.Err)
	}

	secondWriteParams := createRandomSetAdd(secondKey)
	secondWriteReq, secondWriteChan := createStaticWrite(firstWriteReply.TransactionId, firstWriteReply.Timestamp, secondWriteParams)

	go tm.handleStaticTMUpdate(secondWriteReq)
	secondWriteReply := <-secondWriteChan

	if secondWriteReply.Err != nil {
		t.Error("Error on second write: ", secondWriteReply.Err)
	}

	readParams := []ReadObjectParams{createReadObjParams(firstKey), createReadObjParams(secondKey)}
	readReq, readChan := createStaticRead(secondWriteReply.TransactionId, secondWriteReply.Timestamp, readParams)

	go tm.handleStaticTMRead(readReq)
	readReply := <-readChan

	if len(readReply.States[0].(crdt.SetAWValueState).Elems) == 0 || readReply.States[0].(crdt.SetAWValueState).Elems[0] != firstWriteParams[0].UpdateArgs.(crdt.Add).Element {
		t.Error("Read of first key doesn't match")
		t.Error("Received: ", readReply.States[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", firstWriteParams[0].UpdateArgs.(crdt.Add).Element)
	}
	if len(readReply.States[1].(crdt.SetAWValueState).Elems) == 0 || readReply.States[1].(crdt.SetAWValueState).Elems[0] != secondWriteParams[0].UpdateArgs.(crdt.Add).Element {
		t.Error("Read of second key doesn't match")
		t.Error("Received: ", readReply.States[1].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", secondWriteParams[0].UpdateArgs.(crdt.Add).Element)
	}

}

/*
Goal: do 3 writes in two partitions. 1st write: initial clock, 1st partition. 2nd write: 1st write's clock, 2nd partition.
3rd write: 2nd write's clock, 1st partition
Success: if all operations commit and a read returns the values written.
*/
func TestWrites2(t *testing.T) {
	tm := Initialize(0)

	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)
	firstKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")
	secondKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")

	//We want to force the two keys to go to different partitions
	for GetChannelKey(secondKey) == GetChannelKey(firstKey) {
		fmt.Println("Channel key collision, generating new key...")
		secondKey.Key = string(fmt.Sprint(rand.Uint64()))
	}

	firstWriteParams := createRandomSetAdd(firstKey)
	firstWriteReq, firstWriteChan := createStaticWrite(TransactionId(rand.Uint64()), clocksi.NewClockSiTimestamp(), firstWriteParams)

	//fmt.Println("Sending 1st write")
	go tm.handleStaticTMUpdate(firstWriteReq)
	firstWriteReply := <-firstWriteChan
	//fmt.Println("Got 1st write reply")

	if firstWriteReply.Err != nil {
		t.Error("Error on first write: ", firstWriteReply.Err)
	}

	secondWriteParams := createRandomSetAdd(secondKey)
	secondWriteReq, secondWriteChan := createStaticWrite(firstWriteReply.TransactionId, firstWriteReply.Timestamp, secondWriteParams)

	//fmt.Println("Sending 2nd write")
	go tm.handleStaticTMUpdate(secondWriteReq)
	secondWriteReply := <-secondWriteChan
	//fmt.Println("Got 2nd write reply")

	if secondWriteReply.Err != nil {
		t.Error("Error on second write: ", secondWriteReply.Err)
	}

	thirdWriteParams := createRandomSetAdd(firstKey)
	thirdWriteReq, thirdWriteChan := createStaticWrite(secondWriteReply.TransactionId, secondWriteReply.Timestamp, thirdWriteParams)

	//fmt.Println("Sending 3rd write")
	go tm.handleStaticTMUpdate(thirdWriteReq)
	thirdWriteReply := <-thirdWriteChan
	//fmt.Println("Got 3rd write reply")

	if thirdWriteReply.Err != nil {
		t.Error("Error on third write: ", thirdWriteReply.Err)
	}

	readParams := []ReadObjectParams{createReadObjParams(firstKey), createReadObjParams(secondKey)}
	readReq, readChan := createStaticRead(thirdWriteReply.TransactionId, thirdWriteReply.Timestamp, readParams)

	//fmt.Println("Sending read")
	go tm.handleStaticTMRead(readReq)
	readReply := <-readChan
	//fmt.Println("Got read reply")

	firstKeyWrites := []UpdateObjectParams{firstWriteParams[0], thirdWriteParams[0]}
	if !checkWriteReadSetMatch(readReply.States[0].(crdt.SetAWValueState), firstKeyWrites) {
		t.Error("Read of first key doesn't match")
		t.Error("Received: ", readReply.States[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", firstKeyWrites)
	}
	if !checkWriteReadSetMatch(readReply.States[1].(crdt.SetAWValueState), secondWriteParams) {
		t.Error("Read of second key doesn't match")
		t.Error("Received: ", readReply.States[1].(crdt.SetAWValueState).Elems[0])
		t.Error("Expected: ", secondWriteParams[0].UpdateArgs.(crdt.Add).Element)
	}
}

/*
Goal: do 3 writes in the same partition, with the last write using an early clock. 1st write: initial clock. 2nd write: 1st write's clock. 3rd write: initial clock.
Success: if all operations commit and a read returns the values written.
*/
func TestWrites3(t *testing.T) {
	tm := Initialize(0)

	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)

	firstKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")

	firstWriteParams := createRandomSetAdd(firstKey)
	firstWriteReq, firstWriteChan := createStaticWrite(TransactionId(0), clocksi.NewClockSiTimestamp(), firstWriteParams)

	go tm.handleStaticTMUpdate(firstWriteReq)
	firstWriteReply := <-firstWriteChan

	if firstWriteReply.Err != nil {
		t.Error("Error on first write: ", firstWriteReply.Err)
	}

	secondWriteParams := createRandomSetAdd(firstKey)
	secondWriteReq, secondWriteChan := createStaticWrite(firstWriteReply.TransactionId, firstWriteReply.Timestamp, secondWriteParams)

	go tm.handleStaticTMUpdate(secondWriteReq)
	secondWriteReply := <-secondWriteChan

	if secondWriteReply.Err != nil {
		t.Error("Error on second write: ", secondWriteReply.Err)
	}

	thirdWriteParams := createRandomSetAdd(firstKey)
	thirdWriteReq, thirdWriteChan := createStaticWrite(TransactionId(0), clocksi.NewClockSiTimestamp(), thirdWriteParams)

	go tm.handleStaticTMUpdate(thirdWriteReq)
	thirdWriteReply := <-thirdWriteChan

	if thirdWriteReply.Err != nil {
		t.Error("Error on third write: ", thirdWriteReply.Err)
	}

	readParams := []ReadObjectParams{createReadObjParams(firstKey)}
	readReq, readChan := createStaticRead(secondWriteReply.TransactionId, secondWriteReply.Timestamp, readParams)

	go tm.handleStaticTMRead(readReq)
	readReply := <-readChan

	firstKeyWrites := []UpdateObjectParams{firstWriteParams[0], secondWriteParams[0], thirdWriteParams[0]}
	if !checkWriteReadSetMatch(readReply.States[0].(crdt.SetAWValueState), firstKeyWrites) {
		t.Error("Read of key doesn't match")
		t.Error("Received: ", readReply.States[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", firstKeyWrites)
	}
}

/*
Goal: to test if multiple writes and reads to the same key finish succesfully with the expected results. This includes writing with old clocks and reading with clocks higher than the latest's commit
Success: if every operation commits and final read returns all values written.
1st: write in [0], with [0]
	- writes…
	- commit
2nd: write in [0], with [2]
	- writes…
	- commit
3rd: write in [0], with [1]
	- writes…
	- commit
5th: read with [4]
	- check that it doesn’t commit
6th: write with [3]
	- writes…
	- commit
	- check that 5th finished now.

*/
func TestWritesAndReads(t *testing.T) {
	tm := Initialize(0)

	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)
	firstKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")

	firstWriteParams := createRandomSetAdd(firstKey)
	firstWriteReq, firstWriteChan := createStaticWrite(TransactionId(rand.Uint64()), clocksi.NewClockSiTimestamp(), firstWriteParams)

	go tm.handleStaticTMUpdate(firstWriteReq)
	firstWriteReply := <-firstWriteChan

	if firstWriteReply.Err != nil {
		t.Error("Error on first write: ", firstWriteReply.Err)
	}

	secondWriteParams := createRandomSetAdd(firstKey)
	secondWriteReq, secondWriteChan := createStaticWrite(firstWriteReply.TransactionId, firstWriteReply.Timestamp.NextTimestamp(0), secondWriteParams)

	go tm.handleStaticTMUpdate(secondWriteReq)
	secondWriteReply := <-secondWriteChan

	if secondWriteReply.Err != nil {
		t.Error("Error on second write: ", secondWriteReply.Err)
	}

	thirdWriteParams := createRandomSetAdd(firstKey)
	thirdWriteReq, thirdWriteChan := createStaticWrite(firstWriteReply.TransactionId, firstWriteReply.Timestamp, thirdWriteParams)

	go tm.handleStaticTMUpdate(thirdWriteReq)
	thirdWriteReply := <-thirdWriteChan

	if thirdWriteReply.Err != nil {
		t.Error("Error on third write: ", thirdWriteReply.Err)
	}

	var futureTs clocksi.Timestamp
	if thirdWriteReply.Timestamp.IsHigherOrEqual(thirdWriteReply.Timestamp) {
		futureTs = thirdWriteReply.Timestamp.NextTimestamp(0)
	} else {
		futureTs = thirdWriteReply.Timestamp.NextTimestamp(0)
	}
	readParams := []ReadObjectParams{createReadObjParams(firstKey)}
	firstReadReq, firstReadChan := createStaticRead(TransactionId(0), futureTs, readParams)

	go tm.handleStaticTMRead(firstReadReq)

	//Reads for timestamps more recent than latest' commit no longer are supposed to block, unless there's a commit pending.
	/*
		//This read is supposed to timeout, as we asked for a timestamp that doesn't yet exist.
		select {
		case <-firstReadChan:
			t.Error("Error - the first read didn't block, even though the timestamp used is not yet available.")
			return
		case <-time.After(2 * time.Second):
			t.Log("First read timeout, as expected.")
		}
	*/

	firstReadReply := <-firstReadChan
	firstReadWrites := []UpdateObjectParams{firstWriteParams[0], secondWriteParams[0], thirdWriteParams[0]}
	if !checkWriteReadSetMatch(firstReadReply.States[0].(crdt.SetAWValueState), firstReadWrites) {
		t.Error("First read of key doesn't match")
		t.Error("Received: ", firstReadReply.States[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", firstReadWrites)
	}

	fourthWriteParams := createRandomSetAdd(firstKey)
	fourthWriteReq, fourthWriteChan := createStaticWrite(thirdWriteReply.TransactionId, thirdWriteReply.Timestamp, fourthWriteParams)

	go tm.handleStaticTMUpdate(fourthWriteReq)
	fourthWriteReply := <-fourthWriteChan

	if fourthWriteReply.Err != nil {
		t.Error("Error on fourth write: ", fourthWriteReply.Err)
	}

	secondReadReq, secondReadChan := createStaticRead(fourthWriteReply.TransactionId, fourthWriteReply.Timestamp, readParams)

	go tm.handleStaticTMRead(secondReadReq)
	secondReadReply := <-secondReadChan

	secondReadWrites := []UpdateObjectParams{firstWriteParams[0], secondWriteParams[0], thirdWriteParams[0], fourthWriteParams[0]}
	if !checkWriteReadSetMatch(secondReadReply.States[0].(crdt.SetAWValueState), secondReadWrites) {
		t.Error("Second read of key doesn't match")
		t.Error("Received: ", secondReadReply.States[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", secondReadWrites)
	}
}

//Tests the following sequence of operations: startTxn -> write -> read -> write -> commit
func TestNonStaticTransaction1(t *testing.T) {
	tm := Initialize(0)
	txnPartitions := &ongoingTxn{}
	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)

	txnRep := createAndProcessStartTxn(tm, txnPartitions, TransactionId(rand.Uint64()), clocksi.NewClockSiTimestamp())
	fmt.Println(txnRep.TransactionId)

	firstKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")
	firstWriteParams, firstWriteReply := createAndProcessWrite(tm, txnPartitions, nonStatic, firstKey, createRandomSetAdd, txnRep.TransactionId, txnRep.Timestamp)
	checkUpdateError(1, firstWriteReply.updateReply, t)

	readKeyParams := []ReadObjectParams{createReadObjParams(firstKey)}
	readReply := createAndProccessRead(tm, nonStatic, readKeyParams, txnRep.TransactionId, txnRep.Timestamp)
	if !checkWriteReadSetMatch(readReply.readReply[0].(crdt.SetAWValueState), firstWriteParams) {
		t.Error("Read of key doesn't match")
		t.Error("Received: ", readReply.readReply[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", firstWriteParams)
	}
	//TODO: Check that read reflects the previous write effects

	_, secondWriteReply := createAndProcessWrite(tm, txnPartitions, nonStatic, firstKey, createRandomSetAdd, txnRep.TransactionId, txnRep.Timestamp)
	checkUpdateError(2, secondWriteReply.updateReply, t)

	commitRep := createAndProcessCommit(tm, txnPartitions, txnRep.TransactionId, txnRep.Timestamp)
	checkCommitError(1, commitRep, t)
}

//Tests two concurrent non-static transactions.
//After the 2nd txn (with higher startTxn timestamp) commits, we check that a read on the 1st txn doesn't reflect any updates on the 2nd txn.
//We also check that a 3rd txn started after the 1st txn commits succesfully reads the 2nd txn's updates
func TestNonStaticTransaction2(t *testing.T) {
	tm := Initialize(0)
	txnPartitions := &ongoingTxn{}
	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)

	//Initial static update to add some data to a set CRDT
	key := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")
	staticTxnWriteParams, staticTxnWriteReply := createAndProcessWrite(tm, txnPartitions, static, key, createRandomSetAdd, TransactionId(rand.Uint64()), clocksi.NewClockSiTimestamp())
	checkStaticUpdateError(1, staticTxnWriteReply.staticUpdateReply, t)

	firstTxnRep := createAndProcessStartTxn(tm, txnPartitions, staticTxnWriteReply.staticUpdateReply.TransactionId, staticTxnWriteReply.staticUpdateReply.Timestamp)
	secondTxnRep := createAndProcessStartTxn(tm, txnPartitions, staticTxnWriteReply.staticUpdateReply.TransactionId, staticTxnWriteReply.staticUpdateReply.Timestamp)

	//1st txn
	firstTxnWriteParams, firstTxnWriteReply := createAndProcessWrite(tm, txnPartitions, nonStatic, key, createRandomSetAdd, firstTxnRep.TransactionId, firstTxnRep.Timestamp)
	checkUpdateError(1, firstTxnWriteReply.updateReply, t)

	readParams := []ReadObjectParams{createReadObjParams(key)}
	firstTxnReadReply := createAndProccessRead(tm, nonStatic, readParams, firstTxnRep.TransactionId, firstTxnRep.Timestamp)

	readExpectedUpds := []UpdateObjectParams{staticTxnWriteParams[0], firstTxnWriteParams[0]}
	if !checkWriteReadSetMatch(firstTxnReadReply.readReply[0].(crdt.SetAWValueState), readExpectedUpds) {
		t.Error("Read of key doesn't match")
		t.Error("Received: ", firstTxnReadReply.readReply[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", readExpectedUpds)
	}

	//2nd txn
	secondTxnReadReply := createAndProccessRead(tm, nonStatic, readParams, secondTxnRep.TransactionId, secondTxnRep.Timestamp)
	if !checkWriteReadSetMatch(secondTxnReadReply.readReply[0].(crdt.SetAWValueState), staticTxnWriteParams) {
		t.Error("Second txn read of key doesn't match")
		t.Error("Received: ", secondTxnReadReply.readReply[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", staticTxnWriteParams)
	}

	//1st txn
	firstCommitRep := createAndProcessCommit(tm, txnPartitions, firstTxnRep.TransactionId, firstTxnRep.Timestamp)
	checkCommitError(1, firstCommitRep, t)

	//3rd txn
	thirdTxnRep := createAndProcessStartTxn(tm, txnPartitions, firstTxnRep.TransactionId, firstCommitRep.Timestamp)

	thirdTxnReadReply := createAndProccessRead(tm, nonStatic, readParams, thirdTxnRep.TransactionId, thirdTxnRep.Timestamp)
	if !checkWriteReadSetMatch(thirdTxnReadReply.readReply[0].(crdt.SetAWValueState), readExpectedUpds) {
		t.Error("Third txn read of key doesn't match")
		t.Error("Received: ", thirdTxnReadReply.readReply[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", readExpectedUpds)
	}

	//Commit both 2nd and 3rd txns. Order of commit is irrelevant here.
	secondCommitRep := createAndProcessCommit(tm, txnPartitions, secondTxnRep.TransactionId, secondTxnRep.Timestamp)
	checkCommitError(2, secondCommitRep, t)
	thidCommitRep := createAndProcessCommit(tm, txnPartitions, thirdTxnRep.TransactionId, thirdTxnRep.Timestamp)
	checkCommitError(3, thidCommitRep, t)
}

func TestReplicator1(t *testing.T) {
	tm1 := Initialize(0)
	time.Sleep(time.Duration(200) * time.Millisecond)
	tm2 := Initialize(1)
	tm1.replicator.tmpAddRemoteReplicator(tm2.replicator.receiveReplChan)
	tm2.replicator.tmpAddRemoteReplicator(tm1.replicator.receiveReplChan)
	//tm1.replicator.tmpAddRemoteReplicator(make(chan ReplicatorRequest))

	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)

	key := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")
	writeReq, writeReply := createAndProcessWrite(tm1, nil, static, key, createRandomSetAdd, TransactionId(rand.Uint64()), clocksi.NewClockSiTimestamp())
	checkStaticUpdateError(1, writeReply.staticUpdateReply, t)
	ignore(writeReq)
	//ignore(tm2)

	//Wait for replication
	time.Sleep(tsSendDelay * 2 * time.Millisecond)

	readParams := []ReadObjectParams{createReadObjParams(key)}
	fmt.Println("[TM_TEST]Requesting read...")
	firstTxnReadReply := createAndProccessRead(tm2, static, readParams, writeReply.staticUpdateReply.TransactionId, writeReply.staticUpdateReply.Timestamp)
	fmt.Println("[TM_TEST]Received read reply")
	if !checkWriteReadSetMatch(firstTxnReadReply.staticReadReply.States[0].(crdt.SetAWValueState), writeReq) {
		t.Error("Read of key doesn't match")
		t.Error("Received: ", firstTxnReadReply.staticReadReply.States[0].(crdt.SetAWValueState).Elems)
		t.Error("Expected: ", writeReq)
	}

}

/*****METHODS FOR CREATING REQUESTS*****/

func createStaticWrite(txnId TransactionId, ts clocksi.Timestamp, updParams []UpdateObjectParams) (request TransactionManagerRequest, replyChan chan TMStaticUpdateReply) {
	replyChan = make(chan TMStaticUpdateReply)
	request = TransactionManagerRequest{
		TransactionId: txnId,
		Timestamp:     ts,
		Args: TMStaticUpdateArgs{
			UpdateParams: updParams,
			ReplyChan:    replyChan,
		},
	}
	return
}

func createStaticRead(txnId TransactionId, ts clocksi.Timestamp, readParams []ReadObjectParams) (request TransactionManagerRequest, replyChan chan TMStaticReadReply) {
	replyChan = make(chan TMStaticReadReply)
	request = TransactionManagerRequest{
		TransactionId: txnId,
		Timestamp:     ts,
		Args: TMStaticReadArgs{
			ReadParams: readParams,
			ReplyChan:  replyChan,
		},
	}
	return
}

func createWrite(txnId TransactionId, ts clocksi.Timestamp, updParams []UpdateObjectParams) (request TransactionManagerRequest, replyChan chan TMUpdateReply) {
	replyChan = make(chan TMUpdateReply)
	request = TransactionManagerRequest{
		TransactionId: txnId,
		Timestamp:     ts,
		Args: TMUpdateArgs{
			UpdateParams: updParams,
			ReplyChan:    replyChan,
		},
	}
	return
}

func createRead(txnId TransactionId, ts clocksi.Timestamp, readParams []ReadObjectParams) (request TransactionManagerRequest, replyChan chan []crdt.State) {
	replyChan = make(chan []crdt.State)
	request = TransactionManagerRequest{
		TransactionId: txnId,
		Timestamp:     ts,
		Args: TMReadArgs{
			ReadParams: readParams,
			ReplyChan:  replyChan,
		},
	}
	return
}

func createStartTxn(txnId TransactionId, ts clocksi.Timestamp) (request TransactionManagerRequest, replyChan chan TMStartTxnReply) {
	replyChan = make(chan TMStartTxnReply)
	request = TransactionManagerRequest{
		TransactionId: txnId,
		Timestamp:     ts,
		Args:          TMStartTxnArgs{ReplyChan: replyChan},
	}
	return
}

func createCommit(txnId TransactionId, ts clocksi.Timestamp) (request TransactionManagerRequest, replyChan chan TMCommitReply) {
	replyChan = make(chan TMCommitReply)
	request = TransactionManagerRequest{
		TransactionId: txnId,
		Timestamp:     ts,
		Args:          TMCommitArgs{ReplyChan: replyChan},
	}
	return
}

func createRandomSetAdd(keyParams KeyParams) (writeParams []UpdateObjectParams) {
	writeParams = make([]UpdateObjectParams, 1)
	writeParams[0] = UpdateObjectParams{
		KeyParams: keyParams,
		UpdateArgs: crdt.Add{
			Element: crdt.Element(string(fmt.Sprint(rand.Uint64()))),
		},
	}
	return
}

func createReadObjParams(keyParams KeyParams) (readParams ReadObjectParams) {
	return ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.StateReadArguments{}}
}

func createAndProcessCommit(tm *TransactionManager, txnPartitions *ongoingTxn, txnId TransactionId, ts clocksi.Timestamp) (commitReply TMCommitReply) {
	commitReq, commitChan := createCommit(txnId, ts)
	go tm.handleTMCommit(commitReq, txnPartitions)
	return <-commitChan
}

/*****METHODS FOR BOTH CREATING AND PROCESSING REQUESTS (including sending them & waiting for reply)*****/

func createAndProcessWrite(tm *TransactionManager, txnPartitions *ongoingTxn, isStatic bool, key KeyParams, writeParamsFunc func(KeyParams) []UpdateObjectParams, txnId TransactionId,
	ts clocksi.Timestamp) (writeParams []UpdateObjectParams, writeReply testUpdateReply) {
	writeParams = writeParamsFunc(key)
	if isStatic {
		writeReq, writeChan := createStaticWrite(txnId, ts, writeParams)
		go tm.handleStaticTMUpdate(writeReq)
		writeReply = testUpdateReply{staticUpdateReply: <-writeChan}
	} else {
		writeReq, writeChan := createWrite(txnId, ts, writeParams)
		go tm.handleTMUpdate(writeReq, txnPartitions)
		writeReply = testUpdateReply{updateReply: <-writeChan}
	}
	return
}

func createAndProccessRead(tm *TransactionManager, isStatic bool, readParams []ReadObjectParams, txnId TransactionId, ts clocksi.Timestamp) (readReply testReadReply) {
	if isStatic {
		readReq, readChan := createStaticRead(txnId, ts, readParams)
		go tm.handleStaticTMRead(readReq)
		readReply = testReadReply{staticReadReply: <-readChan}
	} else {
		readReq, readChan := createRead(txnId, ts, readParams)
		go tm.handleTMRead(readReq)
		readReply = testReadReply{readReply: <-readChan}
	}
	return
}

func createAndProcessStartTxn(tm *TransactionManager, txnPartitions *ongoingTxn, txnId TransactionId, ts clocksi.Timestamp) (reply TMStartTxnReply) {
	txn, txnChan := createStartTxn(TransactionId(rand.Uint64()), clocksi.NewClockSiTimestamp())
	go tm.handleTMStartTxn(txn, txnPartitions)
	return <-txnChan
}

/*****OTHERS*****/

//Note: assumes that each write in UpdateObjectParams contains only one update
func checkWriteReadSetMatch(state crdt.SetAWValueState, writeParams []UpdateObjectParams) (ok bool) {
	if len(state.Elems) != len(writeParams) {
		return false
	}
	for _, upd := range writeParams {
		ok = false
		switch typedUpd := upd.UpdateArgs.(type) {
		case crdt.Add:
			for _, elem := range state.Elems {
				if elem == typedUpd.Element {
					ok = true
				}
			}
		case crdt.AddAll:
			for _, addElem := range typedUpd.Elems {
				ok = false
				for _, elem := range state.Elems {
					if elem == addElem {
						ok = true
					}
				}
				if !ok {
					return false
				}
			}
		}
		if !ok {
			return false
		}
	}
	return ok
}

/*****ERROR CHECKING UTILS*****/
func checkUpdateError(nWrite int, updReply TMUpdateReply, t *testing.T) {
	if updReply.Err != nil {
		t.Error("Error on write", nWrite, ":", updReply.Err)
	}
}

func checkStaticUpdateError(nWrite int, updReply TMStaticUpdateReply, t *testing.T) {
	if updReply.Err != nil {
		t.Error("Error on static write", nWrite, ":", updReply.Err)
	}
}

func checkCommitError(nCommit int, commitReply TMCommitReply, t *testing.T) {
	if commitReply.Err != nil {
		t.Error("Error on commit", nCommit, ":", commitReply.Err)
	}
}
