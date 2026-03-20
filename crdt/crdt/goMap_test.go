package crdt

import (
	"fmt"
	"os"
	"potionDB/crdt/proto"
	"runtime"
	"strconv"
	"testing"
)

//Tests go maps performance for big map sizes. 1k and 100k testKeys are used as a baseline
//Objects are CRDTs, to mimic the utilization on a RWMap.
//We evaluate for up to 500M testKeys, as lineitems' CRDT map will have more than 200M testKeys.
//(Actual test in R1 did 216061614 (216M) entries)

const MAX_KEY_POS = 400000000

var testData []byte
var testKeys []string
var testMap map[string]CRDT
var currPos int

func initialize() {
	initializeData()
	initializeKeys(5 * 10e7)
}

func initializeData() {
	testData = make([]byte, 40) //Size of a lineitem
	for i := 0; i < 40; i++ {
		testData[i] = byte(i * 43) //Meaningless testData, that will overflow on purpose.
	}
}

func fillData(endPos int) {
	//start, startPos := time.Now().UnixNano(), currPos
	if endPos > MAX_KEY_POS {
		fmt.Printf("Asked to fill more than MAX_KEY_POS (%d). Exiting.\n", MAX_KEY_POS)
		os.Exit(0)
	}
	for ; currPos < endPos; currPos++ {
		testCRDT := InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
		testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(currPos * 2)})
		testMap[testKeys[currPos]] = testCRDT
	}
	//end := time.Now().UnixNano()
	//fmt.Printf("Filled data from pos %d to %d (%d positions). CurrPos: %d. Took %d ms.\n", startPos, endPos, endPos-startPos, currPos, (end-start)/1000000)
}

//testKeys will follow the pattern of lineitems.
/*func makeKeys(nKeys int) (testKeys []string) {
	testKeys = make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		testKeys[i] = strconv.Itoa(i*2) + "_" + string(rune(i % 4 + 1) + '0')
	}
	return testKeys
}*/
/*func initializeKeys(nKeys int) {
	testKeys = make([]string, nKeys)
	fmt.Printf("Initializing %d test keys...\n", nKeys)
	for i := 0; i < nKeys; i++ {
		testKeys[i] = strconv.Itoa(i*2) + "_" + string(rune(i%4+1)+'0')
	}
	fmt.Printf("Finished initializing %d test keys.\n", nKeys)
}*/
func initializeKeys(nKeys int) {
	testKeys = make([]string, nKeys)
	nGoroutines := runtime.NumCPU()
	doneChan, keysPerRoutine := make(chan bool, nGoroutines), nKeys/nGoroutines
	fmt.Printf("Initializing %d test keys with %d goroutines...\n", nKeys, nGoroutines)
	for i := 0; i < nGoroutines-1; i++ {
		go func(start int) {
			for j := start; j < start+keysPerRoutine; j++ {
				testKeys[j] = strconv.Itoa(j*2) + "_" + string(rune(j%4+1)+'0')
			}
			doneChan <- true
		}(i * keysPerRoutine)
	}
	//Last goroutine
	go func(start int) {
		for j := start; j < nKeys; j++ {
			testKeys[j] = strconv.Itoa(j*2) + "_" + string(rune(j%4+1)+'0')
		}
		doneChan <- true
	}((nGoroutines - 1) * keysPerRoutine)
	for i := 0; i < nGoroutines-1; i++ {
		<-doneChan
	}
	fmt.Printf("Finished initializing %d test keys with %d goroutines.\n", nKeys, nGoroutines)
}

/*
for i, pair := range upds {
				//Store effect, even if it's a redo
				effect.Updated[i] = pair.Key
				if !isRedo {
					// Only apply the upd if it isn't a redo (since if it's a redo, we'll just ask the embedded CRDT to rebuild itself by the end)
					embCRDT, new := crdt.getOrCreateEmbCrdt(pair.Key, pair.Upd)
					embDownstream := embCRDT.Downstream(updTs, pair.Upd)
					if new {
						crdt.entries[pair.Key] = embCRDT
						if embCRDT.GetCRDTType() == proto.CRDTType_LWWREG {
							nStringArrays++
						}
					}
					if embDownstream != nil {
						newDown[pair.Key] = embDownstream
					}
				}
			}
*/

/*func (crdt *RWEmbMapCrdt) getOrCreateEmbCrdt(key string, upd UpdateArguments) (embCrdt CRDT, new bool) {
	embCrdt, has := crdt.entries[key]
	new = !has
	if !has {
		embCrdt = InitializeCrdt(upd.GetCRDTType(), crdt.replicaID)
	}
	return
}*/

/*func Benchmark1kGoMap(b *testing.B) {
	testMap =  initialize(1000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 1000) //Need to reset.
	}
}
func Benchmark100kGoMap(b *testing.B) {
	testMap =  initialize(100000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 100000) //Need to reset.
	}
}
func Benchmark1MGoMap(b *testing.B) {
	testMap =  initialize(1000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 1000000) //Need to reset.
	}
}

func Benchmark10MGoMap(b *testing.B) {

	testMap =  initialize(10000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 10000000) //Need to reset.
	}
}

func Benchmark50MGoMap(b *testing.B) {
	testMap =  initialize(5000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 5000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 5000000) //Need to reset.
	}
}

func Benchmark100MGoMap(b *testing.B) {
	testMap =  initialize(100000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 100000000) //Need to reset.
	}
}

func Benchmark200MGoMap(b *testing.B) {
	testMap =  initialize(200000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 200000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 200000000) //Need to reset.
	}
}

func Benchmark300MGoMap(b *testing.B) {
	testMap =  initialize(300000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 300000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 320000000) //Need to reset.
	}
}

func Benchmark400MGoMap(b *testing.B) {
	testMap =  initialize(400000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 400000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 400000000) //Need to reset.
	}
}

func Benchmark500MGoMap(b *testing.B) {
	testMap =  initialize(500000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 500000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 500000000) //Need to reset.
	}
}

func Benchmark1BGoMap(b *testing.B) {
	testMap =  initialize(1000000000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000000; j++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
			testCRDT, has := testMap[testKeys[j]]
			if !has { //Will always be true
				testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
				embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(j * 2)})
				testMap[testKeys[j]] = testCRDT
				if embDownstream != nil { //Will never happen.
					ignore(embDownstream)
				}
			} else {
				ignore(testCRDT)
			}
		}
		testMap = make(map[string]CRDT, 1000000000) //Need to reset.
	}
}*/

/*if testData == nil {
	initialize()
}
testMap = make(map[string]CRDT, 1000)*/
/*
func Benchmark1kGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testCRDT, has := testMap[testKeys[i%1000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%1000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		if i%1000 == 0 {
			testMap = make(map[string]CRDT, 1000) //Need to reset.
		}
	}
}

func Benchmark100kGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		testCRDT, has := testMap[testKeys[i%100000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%100000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		if i%100000 == 0 {
			testMap = make(map[string]CRDT, 100000) //Need to reset.
		}
	}
}

func Benchmark1MGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		testCRDT, has := testMap[testKeys[i%1000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%1000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		if i%1000000 == 0 {
			testMap = make(map[string]CRDT, 1000000) //Need to reset.
		}
	}
}

func Benchmark10MGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 10000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		testCRDT, has := testMap[testKeys[i%10000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%10000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
	}
}

func Benchmark50MGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 50000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		testCRDT, has := testMap[testKeys[i%50000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%50000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
	}
}

func Benchmark100MGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 100000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testCRDT, has := testMap[testKeys[i%100000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%100000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
	}
}

func Benchmark200MGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 200000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testCRDT, has := testMap[testKeys[i%100000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%200000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
	}
}

func Benchmark300MGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 320000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testCRDT, has := testMap[testKeys[i%100000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%300000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
	}
}

func Benchmark400MGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 400000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testCRDT, has := testMap[testKeys[i%100000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%400000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
	}
}

func Benchmark500MGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 500000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testCRDT, has := testMap[testKeys[i%100000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%500000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
	}
}

func Benchmark1BGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 1000000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testCRDT, has := testMap[testKeys[i%100000000]]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[testKeys[i%1000000000]] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
	}
}
*/

/*
func Benchmark1MFullGoMap(b *testing.B) {
	if testData == nil {
		initialize()
		//testMap = make(map[string]CRDT, 10000000)
	}
	testMap, currPos = nil, 0
	runtime.GC()
	testMap = make(map[string]CRDT, 10000000)
	fillData(1000000)
	b.ResetTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark10MFullGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap, currPos = nil, 0
	runtime.GC()
	testMap = make(map[string]CRDT, 20000000)
	fillData(10000000)
	b.ResetTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark50MFullGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap, currPos = nil, 0
	runtime.GC()
	testMap = make(map[string]CRDT, 100000000)
	fillData(50000000)
	b.ResetTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark100MFullGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap, currPos = nil, 0
	runtime.GC()
	testMap = make(map[string]CRDT, 150000000)
	fillData(100000000)
	b.ResetTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark200MFullGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap, currPos = nil, 0
	runtime.GC()
	testMap = make(map[string]CRDT, 250000000)
	fillData(200000000)
	b.ResetTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark300MFullGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap, currPos = nil, 0
	runtime.GC()
	testMap = make(map[string]CRDT, 350000000)
	fillData(300000000)
	b.ResetTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark400MFullGoMap(b *testing.B) {
	if testData == nil {
		initialize()
	}
	testMap, currPos = nil, 0
	runtime.GC()
	testMap = make(map[string]CRDT, 450000000)
	fillData(40000000)
	b.ResetTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}
*/

func Benchmark300M_1MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
		testMap = make(map[string]CRDT, 320000000)
	}
	fillData(1000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}
func Benchmark300M_10MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
		testMap = make(map[string]CRDT, 320000000)
	}
	fillData(10000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}
func Benchmark300M_50MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
		testMap = make(map[string]CRDT, 320000000)
	}
	fillData(50000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}
func Benchmark300M_100MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
		testMap = make(map[string]CRDT, 320000000)
	}
	fillData(100000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}
func Benchmark300M_200MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
		testMap = make(map[string]CRDT, 320000000)
	}
	fillData(200000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}
func Benchmark300M_300MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
		testMap = make(map[string]CRDT, 320000000)
	}
	fillData(300000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark80M_1MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
	}
	testMap = nil
	runtime.GC()
	testMap = make(map[string]CRDT, 80000000)
	fillData(1000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark80M_10MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
	}
	fillData(10000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark80M_50MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
	}
	fillData(50000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}

func Benchmark80M_70MFullGoMap(b *testing.B) {
	b.StopTimer()
	if testData == nil {
		initialize()
	}
	fillData(70000000)
	b.StartTimer()
	var key string
	for i := 0; i < b.N; i++ { //Pattern: check if entry exists; if not add entry. (Yes, the entry will never exist, that's intended.)
		key = testKeys[currPos%MAX_KEY_POS]
		testCRDT, has := testMap[key]
		if !has { //Will always be true
			testCRDT = InitializeCrdt(proto.CRDTType_LWWREG, proto.DATAType_INT, 1)
			embDownstream := testCRDT.Downstream(nil, DownstreamSetValue{NewValue: testData, TsId: tsWithReplicaID(i * 2)})
			testMap[key] = testCRDT
			if embDownstream != nil { //Will never happen.
				ignore(embDownstream)
			}
		} else {
			ignore(testCRDT)
		}
		currPos++
	}
}
