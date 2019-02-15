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
)

//TODO: Lots of common code between different tests... Maybe find common code and extract to one or more methods?

/*
Goal: do 2 writes in different partitions, with the 2nd write using the clock returned by
the 1st write.
Success: if both operations commit and a read returns the values written.
*/
func TestWrites1(t *testing.T) {
	Initialize()

	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)
	firstKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")
	secondKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")

	//We want to force the two keys to go to different partitions
	for GetChannelKey(secondKey) == GetChannelKey(firstKey) {
		secondKey.Key = string(fmt.Sprint(rand.Uint64()))
	}

	firstWriteParams := createRandomSetAdd(firstKey)
	firstWriteReq, firstWriteChan := createWrite(clocksi.NewClockSiTimestamp(), firstWriteParams)

	go handleTMWrite(firstWriteReq)
	firstWriteReply := <-firstWriteChan

	if firstWriteReply.Err != nil {
		t.Error("Error on first write: ", firstWriteReply.Err)
	}

	secondWriteParams := createRandomSetAdd(secondKey)
	secondWriteReq, secondWriteChan := createWrite(firstWriteReply.Timestamp, secondWriteParams)

	go handleTMWrite(secondWriteReq)
	secondWriteReply := <-secondWriteChan

	if secondWriteReply.Err != nil {
		t.Error("Error on second write: ", secondWriteReply.Err)
	}

	readKeysParams := []KeyParams{firstKey, secondKey}
	readReq, readChan := createRead(secondWriteReply.Timestamp, readKeysParams)

	go handleTMRead(readReq)
	readReply := <-readChan

	if len(readReply.States[0].(crdt.SetAWState).Elems) == 0 || readReply.States[0].(crdt.SetAWState).Elems[0] != firstWriteParams[0].UpdateArgs.(crdt.Add).Element {
		t.Error("Read of first key doesn't match")
		t.Error("Received: ", readReply.States[0].(crdt.SetAWState).Elems)
		t.Error("Expected: ", firstWriteParams[0].UpdateArgs.(crdt.Add).Element)
	}
	if len(readReply.States[1].(crdt.SetAWState).Elems) == 0 || readReply.States[1].(crdt.SetAWState).Elems[0] != secondWriteParams[0].UpdateArgs.(crdt.Add).Element {
		t.Error("Read of second key doesn't match")
		t.Error("Received: ", readReply.States[1].(crdt.SetAWState).Elems)
		t.Error("Expected: ", secondWriteParams[0].UpdateArgs.(crdt.Add).Element)
	}

}

/*
Goal: do 3 writes in two partitions. 1st write: initial clock, 1st partition. 2nd write: 1st write's clock, 2nd partition.
3rd write: 2nd write's clock, 1st partition
Success: if all operations commit and a read returns the values written.
*/
func TestWrites2(t *testing.T) {
	go Initialize()

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
	firstWriteReq, firstWriteChan := createWrite(clocksi.NewClockSiTimestamp(), firstWriteParams)

	//fmt.Println("Sending 1st write")
	go handleTMWrite(firstWriteReq)
	firstWriteReply := <-firstWriteChan
	//fmt.Println("Got 1st write reply")

	if firstWriteReply.Err != nil {
		t.Error("Error on first write: ", firstWriteReply.Err)
	}

	secondWriteParams := createRandomSetAdd(secondKey)
	secondWriteReq, secondWriteChan := createWrite(firstWriteReply.Timestamp, secondWriteParams)

	//fmt.Println("Sending 2nd write")
	go handleTMWrite(secondWriteReq)
	secondWriteReply := <-secondWriteChan
	//fmt.Println("Got 2nd write reply")

	if secondWriteReply.Err != nil {
		t.Error("Error on second write: ", secondWriteReply.Err)
	}

	thirdWriteParams := createRandomSetAdd(firstKey)
	thirdWriteReq, thirdWriteChan := createWrite(secondWriteReply.Timestamp, thirdWriteParams)

	//fmt.Println("Sending 3rd write")
	go handleTMWrite(thirdWriteReq)
	thirdWriteReply := <-thirdWriteChan
	//fmt.Println("Got 3rd write reply")

	if thirdWriteReply.Err != nil {
		t.Error("Error on third write: ", thirdWriteReply.Err)
	}

	readKeysParams := []KeyParams{firstKey, secondKey}
	readReq, readChan := createRead(thirdWriteReply.Timestamp, readKeysParams)

	//fmt.Println("Sending read")
	go handleTMRead(readReq)
	readReply := <-readChan
	//fmt.Println("Got read reply")

	firstKeyWrites := []UpdateObjectParams{firstWriteParams[0], thirdWriteParams[0]}
	if !checkWriteReadSetMatch(readReply.States[0].(crdt.SetAWState), firstKeyWrites) {
		t.Error("Read of first key doesn't match")
		t.Error("Received: ", readReply.States[0].(crdt.SetAWState).Elems)
		t.Error("Expected: ", firstKeyWrites)
	}
	if !checkWriteReadSetMatch(readReply.States[1].(crdt.SetAWState), secondWriteParams) {
		t.Error("Read of second key doesn't match")
		t.Error("Received: ", readReply.States[1].(crdt.SetAWState).Elems[0])
		t.Error("Expected: ", secondWriteParams[0].UpdateArgs.(crdt.Add).Element)
	}
}

/*
Goal: do 3 writes in the same partition, with the last write using an early clock. 1st write: initial clock. 2nd write: 1st write's clock. 3rd write: initial clock.
Success: if all operations commit and a read returns the values written.
*/
func TestWrites3(t *testing.T) {
	Initialize()

	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)

	firstKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")

	firstWriteParams := createRandomSetAdd(firstKey)
	firstWriteReq, firstWriteChan := createWrite(clocksi.NewClockSiTimestamp(), firstWriteParams)

	go handleTMWrite(firstWriteReq)
	firstWriteReply := <-firstWriteChan

	if firstWriteReply.Err != nil {
		t.Error("Error on first write: ", firstWriteReply.Err)
	}

	secondWriteParams := createRandomSetAdd(firstKey)
	secondWriteReq, secondWriteChan := createWrite(firstWriteReply.Timestamp, secondWriteParams)

	go handleTMWrite(secondWriteReq)
	secondWriteReply := <-secondWriteChan

	if secondWriteReply.Err != nil {
		t.Error("Error on second write: ", secondWriteReply.Err)
	}

	thirdWriteParams := createRandomSetAdd(firstKey)
	thirdWriteReq, thirdWriteChan := createWrite(clocksi.NewClockSiTimestamp(), thirdWriteParams)

	go handleTMWrite(thirdWriteReq)
	thirdWriteReply := <-thirdWriteChan

	if thirdWriteReply.Err != nil {
		t.Error("Error on third write: ", thirdWriteReply.Err)
	}

	readKeysParams := []KeyParams{firstKey}
	readReq, readChan := createRead(secondWriteReply.Timestamp, readKeysParams)

	go handleTMRead(readReq)
	readReply := <-readChan

	firstKeyWrites := []UpdateObjectParams{firstWriteParams[0], secondWriteParams[0], thirdWriteParams[0]}
	if !checkWriteReadSetMatch(readReply.States[0].(crdt.SetAWState), firstKeyWrites) {
		t.Error("Read of key doesn't match")
		t.Error("Received: ", readReply.States[0].(crdt.SetAWState).Elems)
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
	Initialize()

	//Sleep for a bit to ensure all gothreads initialize
	time.Sleep(initializeTime * time.Millisecond)
	firstKey := CreateKeyParams(string(fmt.Sprint(rand.Uint64())), CRDTType_ORSET, "bkt")

	firstWriteParams := createRandomSetAdd(firstKey)
	firstWriteReq, firstWriteChan := createWrite(clocksi.NewClockSiTimestamp(), firstWriteParams)

	go handleTMWrite(firstWriteReq)
	firstWriteReply := <-firstWriteChan

	if firstWriteReply.Err != nil {
		t.Error("Error on first write: ", firstWriteReply.Err)
	}

	secondWriteParams := createRandomSetAdd(firstKey)
	secondWriteReq, secondWriteChan := createWrite(firstWriteReply.Timestamp.NextTimestamp(), secondWriteParams)

	go handleTMWrite(secondWriteReq)
	secondWriteReply := <-secondWriteChan

	if secondWriteReply.Err != nil {
		t.Error("Error on second write: ", secondWriteReply.Err)
	}

	thirdWriteParams := createRandomSetAdd(firstKey)
	thirdWriteReq, thirdWriteChan := createWrite(firstWriteReply.Timestamp, thirdWriteParams)

	go handleTMWrite(thirdWriteReq)
	thirdWriteReply := <-thirdWriteChan

	if thirdWriteReply.Err != nil {
		t.Error("Error on third write: ", thirdWriteReply.Err)
	}

	var futureTs clocksi.Timestamp
	if thirdWriteReply.Timestamp.IsHigherOrEqual(thirdWriteReply.Timestamp) {
		futureTs = thirdWriteReply.Timestamp.NextTimestamp()
	} else {
		futureTs = thirdWriteReply.Timestamp.NextTimestamp()
	}
	readKeysParams := []KeyParams{firstKey}
	firstReadReq, firstReadChan := createRead(futureTs, readKeysParams)

	go handleTMRead(firstReadReq)

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
	if !checkWriteReadSetMatch(firstReadReply.States[0].(crdt.SetAWState), firstReadWrites) {
		t.Error("First read of key doesn't match")
		t.Error("Received: ", firstReadReply.States[0].(crdt.SetAWState).Elems)
		t.Error("Expected: ", firstReadWrites)
	}

	fourthWriteParams := createRandomSetAdd(firstKey)
	fourthWriteReq, fourthWriteChan := createWrite(thirdWriteReply.Timestamp, fourthWriteParams)

	go handleTMWrite(fourthWriteReq)
	fourthWriteReply := <-fourthWriteChan

	if fourthWriteReply.Err != nil {
		t.Error("Error on fourth write: ", fourthWriteReply.Err)
	}

	secondReadReq, secondReadChan := createRead(fourthWriteReply.Timestamp, readKeysParams)

	go handleTMRead(secondReadReq)
	secondReadReply := <-secondReadChan

	secondReadWrites := []UpdateObjectParams{firstWriteParams[0], secondWriteParams[0], thirdWriteParams[0], fourthWriteParams[0]}
	if !checkWriteReadSetMatch(secondReadReply.States[0].(crdt.SetAWState), secondReadWrites) {
		t.Error("Second read of key doesn't match")
		t.Error("Received: ", secondReadReply.States[0].(crdt.SetAWState).Elems)
		t.Error("Expected: ", secondReadWrites)
	}
}

func createWrite(ts clocksi.Timestamp, updParams []UpdateObjectParams) (request TransactionManagerRequest, replyChan chan TMUpdateReply) {
	replyChan = make(chan TMUpdateReply)
	request = TransactionManagerRequest{
		TransactionId: TransactionId{
			ClientId:  ClientId(rand.Uint64()),
			Timestamp: ts,
		},
		Args: TMUpdateArgs{
			UpdateParams: updParams,
			ReplyChan:    replyChan,
		},
	}
	return
}

func createRead(ts clocksi.Timestamp, keyParams []KeyParams) (request TransactionManagerRequest, replyChan chan TMReadReply) {
	replyChan = make(chan TMReadReply)
	request = TransactionManagerRequest{
		TransactionId: TransactionId{
			ClientId:  ClientId(rand.Uint64()),
			Timestamp: ts,
		},
		Args: TMReadArgs{
			ObjsParams: keyParams,
			ReplyChan:  replyChan,
		},
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

//Note: assumes that each write in UpdateObjectParams contains only one update
func checkWriteReadSetMatch(state crdt.SetAWState, writeParams []UpdateObjectParams) (ok bool) {
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