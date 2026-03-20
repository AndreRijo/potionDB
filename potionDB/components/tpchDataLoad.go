package components

import (
	fmt "fmt"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"
	"tpch_data_processor/tpch"

	"github.com/AndreRijo/go-tools/src/tools"
)

type DataloadParameters struct {
	Region    int8
	IsTMReady chan bool
	Tm        *TransactionManager
	Sf        float64
	DataLoc   string
	tpch.IndexConfigs
}

const (
	PART_BKT_INDEX   = 5
	CHANNEL_END      = 65535
	ALL_BASE_APPLIED = 15362 //Special code for GC routine stating that PotionDB has finished applying all base data + PotionDB's GC run. Go's GC will be forced.
	MANUAL_GC        = false
)

var (
	data        *tpch.TpchData
	dp          DataloadParameters
	regionFuncs [8]func([]string) int8
	updChan     chan crdt.UpdateObjectParams
	iCfg        tpch.IndexConfigs
	ic          InternalClient //Used by SF=1.
	startTime   int64
	cleanChan   chan int
	gcChan      chan int
	icChans     []chan crdt.UpdateObjectParams
)

/*
Create the tables
Call the createIndex()
Then ???
Ideally, would be nice to have this code in a separate repository...
Maybe can just do this "easily" with go mod? Make the repository public so that it can be automatically downloaded.
*/

//TODO: CRDT_PER_OBJ option

func LoadData(dataP DataloadParameters) {
	start := time.Now()
	startTime = start.UnixNano() / 1000000
	shared.TmpHistoryDisable = true
	fmt.Println("[TPCH-DL]Start time of dataload: ", start.Format("2006-01-02 15:04:05.000"))
	LoadBaseData(dataP)
}

func initCleaning() {
	cleanChan = make(chan int, tpch.NTotalFiles*2)
	//go cleanRoutine()
}

func LoadBaseData(dataP DataloadParameters) {
	data, dp = &tpch.TpchData{TpchConfigs: tpch.TpchConfigs{Sf: dataP.Sf, DataLoc: dataP.DataLoc, IsSingleServer: false}, Tables: &tpch.Tables{NOrders: int(float64(tpch.TableEntries[tpch.ORDERS]) * dp.Sf)}}, dataP
	//Part and lineitem are nil
	regionFuncs = [8]func([]string) int8{data.Tables.CustSliceToRegion, nil, data.Tables.NationSliceToRegion, data.Tables.OrdersSliceToRegion, nil,
		data.Tables.PartSuppSliceToRegion, data.Tables.RegionSliceToRegion, data.Tables.SupplierSliceToRegion}
	data.Initialize()
	//debug.SetMemoryLimit(300 * 1024 * 1024 * 1024)
	debug.SetMemoryLimit(int64(float64(tpch.GetRAMSize()) * 0.9))
	debug.SetGCPercent(-1)
	fmt.Printf("[TPCH-DL]Set GC's target memory limit to %.2fGB\n", float64(tpch.GetRAMSize())*0.9/1024/1024/1024)
	go PrepareBaseDataUpdsAndSend()
	initCleaning() //Not needed if using SF1 methods
	initGC()
	fmt.Println("[TPCH-DL]Starting to prepare base data...")
	//start := time.Now().UnixNano()
	data.PrepareBaseData()
	//end := time.Now()
	//fmt.Printf("[TPCH-DL]Base data read and prepared. Time taken: %dms at %s\n", (end.UnixNano()-start)/int64(time.Millisecond), end.Format("15:04:05.000"))
}

func LoadIndexData() {
	if dp.IndexConfigs.IsGlobal && dp.Region > 0 {
		//Do not do index loading
		return
	}
	dp.RegionsToLoad = []int8{dp.Region}
	dp.ScaleFactor = dp.Sf
	iCfg = dp.IndexConfigs
	if iCfg.IsGlobal {
		iCfg.GlobalUpdsChan = make(chan []crdt.UpdateObjectParams, 1000)
	} else {
		iCfg.LocalUpdsChan = make(chan [][]crdt.UpdateObjectParams, 1000)
	}
	fmt.Printf("[TpchIndex]Checking proc tables @ LoadIndexData (tpchDataLoad)... Lengths: region/nation/supplier/customer/part/partsupp/orders/lineitems: %d|%d|%d|%d|%d|%d|%d|%d\n",
		len(data.Tables.Regions), len(data.Tables.Nations), len(data.Tables.Suppliers), len(data.Tables.Customers), len(data.Tables.Parts), len(data.Tables.PartSupps), len(data.Tables.Orders), len(data.Tables.LineItems))
	tpch.InitializeIndexInfo(iCfg, data.Tables)
	fmt.Printf("[TpchIndex]Finished initializing index info @ LoadIndexData. IsGlobal: %v.\n", dp.IndexConfigs.IsGlobal)
	if dp.IndexConfigs.IsGlobal {
		go SendIndexData()
	} else {
		go SendLocalIndexData()
	}
	tpch.PrepareIndexes() //TODO: Uncomment.
	fmt.Printf("[TPCH-DL]Finished loading and preparing index. My Region: %d. Current time: %v.\n", dp.Region, time.Now().Format("2006-01-02 15:04:05.000"))
}

func SendIndexData() {
	if dp.IndexConfigs.IsGlobal && dp.Region > 0 {
		fmt.Printf("[TPCH-DL]SendIndexData - not doing index loading (isGlobal: %v, region: %d)\n", dp.IndexConfigs.IsGlobal, dp.Region)
		//Do not do index loading
		return
	}
	confirmChan := make(chan bool, len(iCfg.QueryNumbers))
	requestedDataClean := false
	fmt.Printf("[TPCH-DL]SendIndexData. Ready to send upds to TM, number of queries: %d.\n", len(iCfg.QueryNumbers))
	for i := 1; i <= len(iCfg.QueryNumbers); i++ {
		//multiUpdHelper(<-iCfg.GlobalUpdsChan, confirmChan)
		go initDataNonBlockingHelper(<-iCfg.GlobalUpdsChan, confirmChan)
		fmt.Printf("[TPCH-DL]Sent protobuf to TM for query index Q%d\n. Queries sent/applied/total: %d/%d/%d\n", i, i, len(confirmChan), len(iCfg.QueryNumbers))
		if len(iCfg.GlobalUpdsChan) == len(iCfg.QueryNumbers)-i { //All updates are prepared. Ask to clean metadata early.
			fmt.Printf("[TPCH-DL]All protobufs have been prepared, but still need to send to TM %d out of %d queries data. Cleaning metadata regardless at %s.\n", len(iCfg.QueryNumbers)-i, len(iCfg.QueryNumbers), time.Now().Format("2006-01-02 15:04:05.000"))
			requestedDataClean = true
			go data.CleanAll()
		}
		/*fmt.Println("[TPCH-DL]Sent protobuf to TM for query index Q", i)
		fmt.Println("[TPCH-DL]Index configs")
		fmt.Println("[TPCH-DL]IsGlobal:", dp.IsGlobal)
		fmt.Println("[TPCH-DL]UseTopKAll:", dp.UseTopKAll)
		fmt.Println("[TPCH-DL]UseTopSum:", dp.UseTopSum)
		fmt.Println("[TPCH-DL]IndexFullData:", dp.IndexFullData)
		fmt.Println("[TPCH-DL]ScaleFactor:", dp.ScaleFactor)
		fmt.Println("[TPCH-DL]QueryNumbers:", dp.QueryNumbers)
		fmt.Println("[TPCH-DL]RegionsToLoad:", dp.RegionsToLoad)
		*/
	}
	fmt.Printf("[TPCH-DL]All protobufs sent to TM. Waiting for them to finish being applied. Current time: %s.\n", time.Now().Format("2006-01-02 15:04:05.000"))
	for i := 0; i < len(iCfg.QueryNumbers); i++ {
		<-confirmChan
	}
	if !requestedDataClean {
		fmt.Printf("[TPCH-DL]All index updates sent (and applied) by TM. Requesting metadata cleaning now.\n")
		data.CleanAll()
	}
	shared.TmpHistoryDisable = false
	fmt.Println("[TPCH-DL]Time at which all index updates were applied: ", time.Now().Format("2006-01-02 15:04:05.000"))
	fmt.Printf("[TPCH-DL]Forcing GC call as all updates have been applied, at %s.\n", time.Now().Format("2006-01-02 15:04:05.000"))
	runtime.GC()
	fmt.Printf("[TPCH-DL]Returned from GC call at %s.\n", time.Now().Format("2006-01-02 15:04:05.000"))
	runtime.GC()
	runtime.GC()
	fmt.Printf("[TPCH-DL]Returned from 3x GC calls at %s.\n", time.Now().Format("2006-01-02 15:04:05.000"))
}

func SendLocalIndexData() {
	confirmChan := make(chan bool, len(iCfg.QueryNumbers)*len(iCfg.RegionsToLoad))
	for i := 0; i < len(iCfg.QueryNumbers); i++ {
		upds := <-iCfg.LocalUpdsChan
		fmt.Println("[TPCH-DL]Sending protobuf to TM for local query index Q", iCfg.QueryNumbers[i])
		for _, reg := range iCfg.RegionsToLoad {
			multiUpdHelper(upds[reg], confirmChan)
		}
		fmt.Println("[TPCH-DL]Sent protobuf to TM for local query index Q", iCfg.QueryNumbers[i])
	}
	for i := 0; i < len(iCfg.QueryNumbers)*len(iCfg.RegionsToLoad); i++ {
		<-confirmChan
	}
	fmt.Println("[TPCH-DL]Time at which all local index updates were sent: ", time.Now().Format("2006-01-02 15:04:05.000"))
}

func PrepareBaseDataUpdsAndSend() {
	updChan = make(chan crdt.UpdateObjectParams, tpch.NTotalFiles-tpch.GetNFilesOfTable(tpch.LINEITEM)+tpch.GetNFilesOfTable(tpch.LINEITEM)*getLineitemsNRoutines())
	nUpds := cap(updChan)
	confirmChan := make(chan int, nUpds)
	icChans = make([]chan crdt.UpdateObjectParams, tpch.N_TABLES)
	for i := 0; i < tpch.N_TABLES; i++ {
		icChans[i] = make(chan crdt.UpdateObjectParams, GetNUpdatesOfTable(i))
	}
	go startInternalClients(confirmChan)

	go PrepareCrdtUpdatesProcVersion()
	//go AckProcTables() //Temporary method that does not generate CRDT updates for base data.
	fmt.Println("[TPCH-DL]Waiting for TM to be ready...")
	<-dp.IsTMReady //Wait until TM is ready
	//time.Sleep(5 * time.Second)
	start := time.Now()
	fmt.Println("[TPCH-DL]TM ready at: ", start.Format("15:04:05.000"), "Starting to collect and send updates to TM.")
	/*if dp.IndexConfigs.IsGlobal {
		go SendIndexData()
	} else {
		go SendLocalIndexData()
	}*/
	sendInitializers()
	forwardUpdsAndGetConfirms(confirmChan)
}

func startInternalClients(confirmChan chan int) {
	/*clientPerTable = make([]InternalClient, tpch.N_TABLES)
	for i := 0; i < tpch.N_TABLES; i++ {
		clientPerTable[i] = InternalClient{}.Initialize(dp.Tm)
	}*/
	for i := 0; i < tpch.N_TABLES; i++ {
		go clientPerTable(i, confirmChan)
	}
}

func clientPerTable(tableI int, confirmChan chan int) {
	client := InternalClient{}.Initialize(dp.Tm)
	myUpdChan := icChans[tableI]
	nWait := 1
	if tableI == tpch.LINEITEM && data.GetLineItemFilesCount() > 1 {
		nWait = data.GetLineItemFilesCount() * getLineitemsNRoutines()
	} else if tableI == tpch.ORDERS && data.GetOrderFilesCount() > 1 {
		nWait = data.GetOrderFilesCount()
	}
	for i := 0; i < nWait; i++ {
		upd := <-myUpdChan
		//fmt.Printf("[TPCH-DL][Client %d]Got forwarded update from myUpdChan. Requesting TM to apply it. Updates currently in queue: %d\n", tableI, len(myUpdChan))
		if len(myUpdChan) >= 2 {
			toMerge := make([]crdt.UpdateObjectParams, 0, len(myUpdChan))
			for len(myUpdChan) > 0 {
				toMerge = append(toMerge, <-myUpdChan)
			}
			nWait -= (len(toMerge) - 1) //-1 to account for the merged update.
			go mergeUpdBufs(toMerge, myUpdChan)
		}
		client.DoSingleInitialDataUpdate(upd.KeyParams, upd.UpdateArgs)
		fmt.Printf("[TPCH-DL][Client %d]TM has applied the update. Updates currently in queue: %d. Estimated left: %d\n", tableI, len(myUpdChan), nWait-i-1)
		confirmChan <- tableI
	}
	confirmChan <- CHANNEL_END + tableI
	client.CloseClient()
}

func mergeUpdBufs(toMerge []crdt.UpdateObjectParams, myUpdChan chan crdt.UpdateObjectParams) {
	fmt.Printf("[TPCH-DL][Merge]Merging %d updates for key %s\n", len(toMerge), toMerge[0].Key)
	if len(toMerge) > 5 { //Use concat, to avoid multiple re-allocations.
		buf := make([][]crdt.EmbMapUpdate, len(toMerge))
		for i := 0; i < len(toMerge); i++ {
			buf[i] = toMerge[i].UpdateArgs.(crdt.EmbMapFirstUpdate).Upds
		}
		final := slices.Concat(buf...)
		myUpdChan <- crdt.UpdateObjectParams{KeyParams: toMerge[0].KeyParams, UpdateArgs: crdt.EmbMapFirstUpdate{Upds: final}}
	} else {
		final := toMerge[0].UpdateArgs.(crdt.EmbMapFirstUpdate)
		for i := 1; i < len(toMerge); i++ {
			final.Upds = append(final.Upds, toMerge[i].UpdateArgs.(crdt.EmbMapFirstUpdate).Upds...)
		}
		myUpdChan <- crdt.UpdateObjectParams{KeyParams: toMerge[0].KeyParams, UpdateArgs: final}
	}
}

func forwardUpdsAndGetConfirms(confirmChan chan int) {
	nItemConfirm, nOrderConfirm, targetItem, targetOrder := 0, 0, data.GetLineItemFilesCount(), data.GetOrderFilesCount()
	nTablesDone := 0 //Tables for which we've received all confirmations.
	for nTablesDone < tpch.N_TABLES {
		//fmt.Printf("[TPCH-DL][Forward]Waiting for request...\n")
		select { //If we keep track of which updates have been confirmed, we can call GC at strategic times, if memory usage so requires.
		case upd := <-updChan:
			updTable := getUpdTable(upd.KeyParams)
			//fmt.Printf("[TPCH-DL][Forward]Forwarding update for table %s.\n", tpch.TableNames[updTable])
			icChans[updTable] <- upd
		case tableI := <-confirmChan: //Note: TPC-DL uses a blocking method for applying updates in PotionDB. Thus, at this point, we know it has been fully applied.
			if tableI < CHANNEL_END {
				//fmt.Printf("[TPCH-DL][Forward]Got confirmation from TM for table %s.\n", tpch.GetTableName(tableI))
			} else if tableI <= CHANNEL_END+tpch.N_TABLES {
				fmt.Printf("[TPCH-DL][Forward]Table %s is complete. Completed so far: %d/%d\n", tpch.GetTableName(tableI-CHANNEL_END), nTablesDone+1, tpch.N_TABLES)
			} else {
				fmt.Printf("[TPCH-DL][Forward]Got unknown confirmation code %d from TM.\n", tableI)
			}
			if tableI >= CHANNEL_END {
				nTablesDone++
			} else if tableI == tpch.CUSTOMER || tableI == tpch.PARTSUPP || tableI == tpch.PART { //These tables are big enough to suggest a GC call. The GC routine will then check if such a call is needed.
				gcChan <- tableI
			} else {
				isOrderTable, isItemTable := tpch.IsOrderTable(tpch.ORDERS), tpch.IsLineItemTable(tpch.LINEITEM)
				if (isOrderTable && targetOrder == 1) || (isItemTable && targetItem == 1) {
					gcChan <- tableI
				} else if isOrderTable && targetOrder > 1 {
					nOrderConfirm++
					if nOrderConfirm%tpch.NSplitReaderRoutines == 0 { //We read NSplitReaderRoutines order files in parallel. We'll try to sync the cleaning with that.
						gcChan <- tableI
					}
				} else if isItemTable && targetItem > 1 {
					nItemConfirm++
					if nItemConfirm%tpch.NSplitReaderRoutines == 0 { //We read NSplitReaderRoutines item files in parallel. We'll try to sync the cleaning with that.
						gcChan <- tableI
					}
				} //We ignore nation and region tables, as they are quite small.
			}
		}
	}
	//All tables fully processed by PotionDB. Call GC of PotionDB, wait, and then send a special notification to GC routine.
	currTime := time.Now()
	fmt.Printf("[TPCH-DL]All updates sent and confirmed by TM. Time: %s. Time since start (ms): %d.\n", currTime.Format("15:04:05.000"), currTime.UnixNano()/1000000-startTime)
	//data.CleanProcTables()
	//fmt.Printf("[TPCH-DL]Processed tables cleaned.\n")
	//requestPotionDBGC()     //Blocks until complete. No longer needed as RWEmbMapCRDTs will clean themselves and their embedded CRDTs as localFirstUpdates are applied.
	//shared.TmpHistoryDisable = false //TODO: This should be after index loading too.
	//dp.Tm.gc.StartGCTimer()          //TODO: This should later be moved to after index loading, or similar.
	//gcChan <- ALL_BASE_APPLIED	//TODO: Uncomment
}

func initGC() {
	gcChan = make(chan int, tpch.NTotalFiles)
	if MANUAL_GC {
		go doGCRoutine()
	}
}

// TODO: Better incorportate Go's memory allocation stats.
func doGCRoutine() { //Possible improvement: on some key points (e.g., all proc tables done), maybe we could always force a GC call.
	const MB = 1048576
	//TODO: GC calls suggestion when we finish making some processed tables?

	//Possible optimization: have a factor considering the number of "missed GC suggestions"
	//The idea would be that, the more suggestions we receive in gcChan that we ignore, the higher the likehood of next time calling GC.
	//The motivation is that many suggestions in gcChan suggest that there is a lot that can be clean.
	fmt.Printf("[TPCH-DL][GC]Starting GC routine. Total Mem, Available Mem, Free %%: %dMB, %dMB, %d%%.\n", tpch.GetRAMSize()/1024/1024, tpch.GetAvailableRAM()/1024/1024, tpch.GetAvailableRAM()*100/tpch.GetRAMSize())
	lastGCFinish, currTime, timeDiff := int64(0), int64(0), int64(0) //Both in ms
	availMem, totalMem, lastAvailMem, currGoMem, lastGCGoMem, memFactor, goMemFactor := uint64(0), tpch.GetRAMSize(), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)
	callGC, callPotionDBGC, priorityGC := false, false, false //priorityGC: true when, after running GC, the memory usage is significantely higher, which means a lot more garbage to collect.
	lastPotionDBGC := int64(0)
	nGCCalls, nPotionDBGCCalls := 0, 0
	memStats := &runtime.MemStats{} //To read memory stats from Go runtime
	for code := range gcChan {
		fmt.Printf("[TPCH-DL][GC]Received code %d on gcChan. Current time: %s\n", code, time.Now().Format("15:04:05.000"))
		if code == ALL_BASE_APPLIED {
			nGCCalls++
			fmt.Printf("[TPCH-DL][GC]TPC-H base data loading is complete. Calling last GC.\n")
			runtime.GC() //Forced GC, as all base data has been fully processed.
			break        //For now, we stop GC routine here. Later, it should keep going for the indexes.
		}
		callGC, callPotionDBGC = false, false
		availMem, currTime = tpch.GetAvailableRAM(), time.Now().UnixMilli()
		runtime.ReadMemStats(memStats)
		memFactor, timeDiff = availMem*100/totalMem, currTime-lastGCFinish
		if priorityGC && memFactor > 50 {
			callGC = true
		} else if timeDiff < 5000 || memFactor > 50 { //If last GC was very recently (<5s), or memory free is high, always skip.
			continue
		}
		if memFactor <= 65 && timeDiff >= 120000 { //Call GC if the last time was very long ago, as we're still OK with memory (60% usage).
			callGC = true
		} else if memFactor <= 50 && timeDiff >= 90000 { //Memory pressure is moderate (60% used), call GC as long as it was at least 90s ago.
			callGC = true
		} else if memFactor <= 35 && timeDiff >= 60000 { //Memory pressure is significant (70% used), call GC as long as it was at least 60s ago.
			callGC = true
		} else if memFactor <= 25 && timeDiff >= 45000 { //Memory pressure is tending to high (75% used), call GC unless it was recent (40s).
			callGC, callPotionDBGC = true, true
		} else if memFactor <= 20 && timeDiff >= 30000 { //Memory pressure is high (80% used), call GC unless it was very recent (30s).
			callGC, callPotionDBGC = true, true
		} else if memFactor <= 15 && timeDiff >= 15000 { //Memory pressure is very high (85% used), call GC unless it was very, very recent (10s).
			callGC, callPotionDBGC = true, true
		} else if memFactor <= 10 && timeDiff >= 5000 { //Memory pressure is dangerously high (90% used), call GC unless it literally just finished (5s).
			callGC, callPotionDBGC = true, true
		}
		if !callGC { //Factor in some more factors to call GC.
			runtime.ReadMemStats(memStats)
			currGoMem = memStats.HeapAlloc
			if lastGCGoMem > 0 && (currGoMem-lastGCGoMem)*100/totalMem > 20 { //A 20% increase in Go memory usage means we have a good amount of trash to clean, so we call GC.
				callGC = true
			}
		}
		//TODO: Add some logic with Go's memory stats.
		//For now the idea is that we snapshot the memory usage from the end of the last GC,
		if callPotionDBGC && memFactor > 15 && currTime-lastPotionDBGC <= 60000 { //Do not call PotionDB's GC too often, unless memFactor <= 15.
			callPotionDBGC = false
			//fmt.Printf("[TPCH-DL][GC]Skipping PotionDB's GC as last call was %dms ago and memFactor is %d%%.\n", currTime-lastPotionDBGC, memFactor)
		}
		if callGC {
			//If memory pressure is not too high, we call PotionDB's GC first, in order to mark more memory as free. Otherwise, we call it after, as PotionDB's GC is slow for high SFs.
			if memFactor > 15 && callPotionDBGC && !priorityGC {
				/*
					lastPotionDBGC = currTime
					start := time.Now().UnixMilli()
					fmt.Printf("[TPCH-DL][GC]Calling PotionDB's GC before GC with %d%% available memory.\n", memFactor)
					requestPotionDBGC()
					currTime = time.Now().UnixMilli()
					nPotionDBGCCalls++
					fmt.Printf("[TPCH-DL][GC]PotionDB's GC took %dms. Free memory before: %d%%\n", currTime-start, memFactor)*/
			}
			runtime.GC()
			runtime.ReadMemStats(memStats)
			lastGCGoMem = memStats.HeapAlloc
			lastGCFinish, lastAvailMem = time.Now().UnixMilli(), availMem
			availMem = tpch.GetAvailableRAM()
			fmt.Printf("[TPCH-DL][GC]Finished GC. Free RAM: %dMB/%dMB (before: %dMB). Percentage free: %d%% (before: %d%%). Reported Go RAM usage: %dMB/%dMB (%d%%). Time since last GC: %dms, Time taken for GC: %dms.\n",
				availMem/MB, totalMem/MB, lastAvailMem/MB, availMem*100/totalMem, memFactor, lastGCGoMem/MB, totalMem/MB, lastGCGoMem*100/totalMem, timeDiff, lastGCFinish-currTime)
			if (priorityGC || memFactor <= 15) && callPotionDBGC {
				/*lastPotionDBGC = lastGCFinish
				start := time.Now().UnixMilli()
				fmt.Printf("[TPCH-DL][GC]Calling PotionDB's GC after GC with %d%% available memory.\n", memFactor)
				requestPotionDBGC()
				currTime = time.Now().UnixMilli()
				nPotionDBGCCalls++
				fmt.Printf("[TPCH-DL][GC]PotionDB's GC took %dms. Free memory before: %d%%\n", currTime-start, memFactor)*/
			}
			memFactor, goMemFactor = availMem*100/totalMem, lastGCGoMem*100/totalMem
			diffFactor := (lastAvailMem - availMem) * 100 / totalMem
			if lastAvailMem > availMem && (goMemFactor >= 60 && ((diffFactor >= 10) || (diffFactor >= 5 && memFactor <= 15))) { //If available memory decreased by at least 10% of total memory (5% if new free memory <= 15%), we set priorityGC to true.
				fmt.Printf("[TPCH-DL][GC]Warning: Available memory decreased by %d%% after GC. Will call GC soon.\n", diffFactor)
				priorityGC = true
			} else {
				priorityGC = false
			}
			nGCCalls++
		} else {
			priorityGC = false
		}
	}
	fmt.Printf("[TPCH-DL][GC]GC routine finished. Total GC calls: %d. Total PotionDB GC calls: %d. Current time: %s\n", nGCCalls, nPotionDBGCCalls, time.Now().Format("15:04:05.000"))
	time.Sleep(20 * time.Second)
	fmt.Printf("[TPCH-DL][GC]20s after finishing GC routine. Calling one last GC. Current time: %s\n", time.Now().Format("15:04:05.000"))
	runtime.GC()
	fmt.Printf("[TPCH-DL][GC]Last GC finished. Current time: %s\n", time.Now().Format("15:04:05.000"))
	ignore(lastAvailMem)
}

func getUpdTable(keyParams crdt.KeyParams) int {
	if keyParams.Key[0] == tpch.TableNames[tpch.LINEITEM][0] {
		return tpch.LINEITEM
	} else if keyParams.Key[0] == tpch.TableNames[tpch.ORDERS][0] {
		return tpch.ORDERS
	} else if keyParams.Key[0] == tpch.TableNames[tpch.CUSTOMER][0] {
		return tpch.CUSTOMER
	} else if keyParams.Key[0] == tpch.TableNames[tpch.PART][0] && len(keyParams.Key) == len(tpch.TableNames[tpch.PART]) { //To distinguish from PARTSUPP
		return tpch.PART
	} else if keyParams.Key[0] == tpch.TableNames[tpch.PARTSUPP][0] {
		return tpch.PARTSUPP
	} else if keyParams.Key[0] == tpch.TableNames[tpch.SUPPLIER][0] {
		return tpch.SUPPLIER
	} else if keyParams.Key[0] == tpch.TableNames[tpch.NATION][0] {
		return tpch.NATION
	} else if keyParams.Key[0] == tpch.TableNames[tpch.REGION][0] {
		return tpch.REGION
	}
	return -1 //Should never happen, better return -1 and crash.
}

func sendInitializers() (nExtraUpds int) {
	//Lineitems initializer. This one we do always as we also support partitioning the making of lineitem updates.
	/*go updateHelperNoConfirmation(crdt.UpdateObjectParams{
		KeyParams:  crdt.MakeKeyParams(tpch.TableNames[tpch.LINEITEM], proto.CRDTType_RRMAP, tpch.Buckets[dp.Region]),
		UpdateArgs: crdt.EmbMapInit(int(float64(tpch.TableEntries[tpch.LINEITEM]) * 0.35))})
	nExtraUpds++
	//Orders initializer
	if data.GetOrderFilesCount() > 1 {
		go updateHelperNoConfirmation(crdt.UpdateObjectParams{
			KeyParams:  crdt.MakeKeyParams(tpch.TableNames[tpch.ORDERS], proto.CRDTType_RRMAP, tpch.Buckets[dp.Region]),
			UpdateArgs: crdt.EmbMapInit(int(float64(tpch.TableEntries[tpch.ORDERS]) * data.Sf * 0.201))})
		nExtraUpds++
	}*/
	upds := []crdt.UpdateObjectParams{{
		KeyParams:  crdt.MakeKeyParams(tpch.TableNames[tpch.LINEITEM], proto.CRDTType_RRMAP, tpch.Buckets[dp.Region]),
		UpdateArgs: crdt.EmbMapInit{Size: int(float64(tpch.TableEntries[tpch.LINEITEM]) * 0.35)}}, {

		KeyParams:  crdt.MakeKeyParams(tpch.TableNames[tpch.ORDERS], proto.CRDTType_RRMAP, tpch.Buckets[dp.Region]),
		UpdateArgs: crdt.EmbMapInit{Size: int(float64(tpch.TableEntries[tpch.ORDERS]) * data.Sf * 0.201)}},
	}
	//fmt.Printf("[TPCH-DL]Sending initializers for lineitem and orders...\n")
	initDataHelperNoConfirmation(upds)
	//fmt.Printf("[TPCH-DL]Finished sending initializers.\n")
	nExtraUpds += len(upds)
	return
}

/*func PrepareBaseDataUpdsAndSend() {
	//updChan = make(chan crdt.UpdateObjectParams, len(tpch.TableNames)-1+getLineitemsNRoutines()*data.GetLineItemFilesCount())
	updChan = make(chan crdt.UpdateObjectParams, tpch.NTotalFiles-tpch.GetNFilesOfTable(tpch.LINEITEM)+tpch.GetNFilesOfTable(tpch.LINEITEM)*getLineitemsNRoutines())
	nUpds := cap(updChan)
	confirmChan := make(chan bool, nUpds)
	//go PrepareCrdtUpdates()
	go PrepareCrdtUpdatesProcVersion()
	fmt.Println("[TPCH-DL]Waiting for TM to be ready...")
	<-dp.IsTMReady //Wait until TM is ready
	start := time.Now()
	fmt.Println("[TPCH-DL]TM ready at: ", start.Format("15:04:05.000"), "Starting to collect and send updates to TM.")
	if dp.IndexConfigs.IsGlobal {
		go SendIndexData()
	} else {
		go SendLocalIndexData()
	}
	nUpds += sendInitializers
	fmt.Println("[TPCH-DL]Waiting for updChan...")
	for i := 0; i < cap(updChan); i++ {
		go updateHelper(<-updChan, confirmChan)
		if i%5 == 0 && cap(updChan)-i >= 10 {
			fmt.Printf("[TPCH-DL]Got something on updChan, called updateHelper %d/%d.\n", i, cap(updChan)-1)
		} else if cap(updChan)-i < 10 {
			fmt.Printf("[TPCH-DL]Got something on updChan, called updateHelper %d/%d.\n", i, cap(updChan)-1)
		}
		//fmt.Printf("[TPCH-DL]Got something on updChan, called updateHelper %d/%d.\n", i, cap(updChan)-1)
	}
	data.CleanProcTables() //TODO: Remove this.
	fmt.Printf("[TPCH-DL]Got all updates on updChan, waiting for TM to confirm %d updates.\n", nUpds)
	for i := 0; i < nUpds; i++ {
		<-confirmChan
		if i%10 == 0 && nUpds-i >= 10 {
			fmt.Printf("[TPCH-DL]Got confirmation %d/%d.\n", i, nUpds-1)
		} else if nUpds-i < 10 {
			fmt.Printf("[TPCH-DL]Got confirmation %d/%d.\n", i, nUpds-1)
		}
		//fmt.Printf("[TPCH-DL]Got confirmation %d/%d.\n", i, nUpds-1)
	}
	fmt.Printf("[TPCH-DL]Done, TM last commit at %s. Time taken: %dms\n", time.Now().Format("15:04:05.000"), (time.Now().UnixNano()-start.UnixNano())/int64(time.Millisecond))
}*/

// Intended for debugging/testing purposes only. Use this to replace PrepareCrdtUpdatesProcVersion()
// It receives the processed tables and calls LoadIndexData() when appropriate, but it does not generate CRDT updates for base data.
func AckProcTables() {
	fmt.Printf("[TPCH-DL]Warning - not generating updates for base data. Will only acknowledge processed tables.\n")

	procTablesOrder := make([]int8, tpch.NTotalFiles)
	for i := 0; i < tpch.NTotalFiles; i++ {
		tableN := <-data.ProcChan
		procTablesOrder[i] = int8(tableN)
		gcChan <- tableN
		data.CleanTableRawData(tableN)
	}
	gcChan <- ALL_BASE_APPLIED
	fmt.Printf("[TPCH-DL]All processed tables received. Order: %v\n", procTablesOrder)
	close(data.Read2Chan)
	data.Read2Chan = nil
	data.FinishCleanTableRawData()
	if dp.QueryNumbers != nil {
		go LoadIndexData() //TODO: Start loading some views earlier? (i.e., views that do not need all tables)
	}
	//TODO: shared.TmpHistoryDisable = false?
}

// In this version we use Tables (from tpchTables.go) to prepare the updates, using memory-efficient representation of each field of each table.
func PrepareCrdtUpdatesProcVersion() {
	fmt.Println("[TPCH-DL]Starting to prepare updates... (Proc version)")
	//Need 3 types:
	//Replicated everywhere (parts). Each server processes a part of it (pun intended).
	//Replicated locally (order, supplier, itemsupply, partsupp, nation, region)
	//Replicated locally + one other place (lineitem)/*

	//Requirements (of processed tables) for each table that we want to make updates for:
	//cust: nation, region
	//lineitem: supplier, order, customer, nation
	//nation: region
	//orders: customer, nation
	//part: N/A
	//partsupp: supplier, nation
	//region: N/A
	//supplier: nation
	//Note: Region and Nation will ALWAYS be the first two (by this order) to come out of procChan. So we never need to check if we already have them.
	//Also, when both orders and items are split, the respective order will ALWAYS come first (as order and items are read as pairs, with order always being first.)
	var tableN int
	procSupp, procCust, waitingPartSupp, procFullOrders := false, false, false, false
	nOrders, nItems := data.GetOrderFilesCount(), data.GetLineItemFilesCount()
	//ItemsDone is not needed, just for debugging.
	waitingOrders, ordersDone, waitingItems, itemsDone := tools.NewBitSet(nOrders), tools.NewBitSet(nOrders), tools.NewBitSet(nItems), tools.NewBitSet(nItems)
	var readyStart time.Time
	offset, startOrder, endOrder, nOrderRec, nItemRec := 0, 0, 0, 0, 0 //The last two are used to control GC suggestions.
	//receivedItems, receivedOrders := make([]bool, nItems), make([]bool, nOrders)

	for i := 0; i < tpch.NTotalFiles; i++ {
		tableN = <-data.ProcChan
		if readyStart.IsZero() {
			readyStart = time.Now()
			fmt.Printf("[TPCH-DL]Ready to prepare CRDT updates at %s. Time since start: %dms.\n", readyStart.Format("15:04:05.000"), (readyStart.UnixNano()/1000000)-startTime)
		}
		fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Got table number %d (Table %s, %d/%d) from data.ProcChan.\n", tableN, tpch.GetTableName(tableN), tpch.GetSplitOffset(tableN), tpch.GetNFilesOfTable(tableN)-1)
		if tableN == tpch.PART {
			go makeCRDTUpdatesFromProcTables(0, tableN, 0, data.GetTableSize(tableN), data.GetTableSize(tableN), tpch.Buckets[PART_BKT_INDEX])
			gcChan <- tableN
		} else if tpch.IsLineItemTable(tableN) { //Wait for customers, suppliers and orders.
			nItemRec++
			offset = tpch.GetSplitOffset(tableN)
			if !procCust || !procSupp || (nOrders == 1 && !procFullOrders) || (nOrders > 1 && !ordersDone.GetBit(offset)) { //Despite an item file always being read after the respective order file finishes, the same is not guaranteed for processing.
				waitingItems.Set(offset)
			} else {
				if tableN == tpch.LINEITEM {
					go makeCRDTUpdatesFromProcTables(0, tpch.LINEITEM, 0, data.GetTableSize(tpch.LINEITEM), int(data.SplitNLineItems[offset].Load()), tpch.Buckets[dp.Region])
					gcChan <- tableN
				} else {
					go makeCRDTUpdatesFromProcTables(offset, tpch.LINEITEM, int(data.SplitFilesStartPos[offset].Load()), int(data.SplitFilesStartPos[offset+1].Load()), int(data.SplitNLineItems[offset].Load()), tpch.Buckets[dp.Region])
					itemsDone.Set(offset)
					if nItemRec%tpch.NSplitReaderRoutines == 0 { //We read NSplitReaderRoutines item files in parallel. We'll try to sync the cleaning with that.
						gcChan <- tableN
					}
				}
			}
			//TMP logic: Always delay lineitems, process at end. See how it goes.
			/*waitingItems.Set(offset)
			receivedItems[offset] = true
			if nOrders > 1 && !receivedOrders[offset] { //If orders are split, then the respective order is always processed before the items.
				fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Fatal error - received lineitem file %d but not yet order file %d. Exiting.\n", offset, offset)
				panic(1)
			}*/
		} else if tpch.IsOrderTable(tableN) { //Wait for customers
			nOrderRec++
			if tableN == tpch.ORDERS { //Not split.
				go makeCRDTUpdatesFromProcTables(0, tpch.ORDERS, 0, data.GetTableSize(tpch.ORDERS), data.GetTableSize(tpch.ORDERS), tpch.Buckets[dp.Region])
				procFullOrders = true
				if nItems > 1 {
					for i := 0; i < nItems; i++ {
						if waitingItems.GetBit(i) {
							go makeCRDTUpdatesFromProcTables(i, tpch.LINEITEM, int(data.SplitFilesStartPos[i].Load()), int(data.SplitFilesStartPos[i+1].Load()), int(data.SplitNLineItems[i].Load()), tpch.Buckets[dp.Region])
							itemsDone.Set(i)
						}
					}
				}
				gcChan <- tableN
			} else {
				offset = tpch.GetSplitOffset(tableN)
				ordersDone.Set(offset)
				if !procCust { //Need to wait for customers
					waitingOrders.Set(offset)
				} else {
					//waitingOrders.Set(offset)      	//TMP - So that lineitems does not give false positives.                                                                                       //TMP - So that lineitems does not give false positives.                                                                                            //TMP - So that lineitems does not give false positives.
					startOrder, endOrder = int(data.SplitFilesStartPos[offset].Load())+1, int(data.SplitFilesStartPos[offset+1].Load()) //+1 as orders are 1 position ahead of items (end is accurate though)
					go makeCRDTUpdatesFromProcTables(offset, tpch.ORDERS, startOrder, endOrder, endOrder-startOrder, tpch.Buckets[dp.Region])
					if waitingItems.GetBit(offset) && procSupp {
						go makeCRDTUpdatesFromProcTables(offset, tpch.LINEITEM, startOrder-1, endOrder-1, int(data.SplitNLineItems[offset].Load()), tpch.Buckets[dp.Region])
						itemsDone.Set(offset)
					}
					if nOrderRec%tpch.NSplitReaderRoutines == 0 { //We read NSplitReaderRoutines item files in parallel. We'll try to sync the cleaning with that.
						gcChan <- tableN
					}
				}
				//receivedOrders[offset] = true
			}
		} else if tableN == tpch.PARTSUPP { //May have to wait for Supplier. But I'd expect usually not, as Supplier table is smaller.
			if !procSupp { //Need to wait for suppliers.
				waitingPartSupp = true
			} else {
				go makeCRDTUpdatesFromProcTables(0, tableN, 0, data.GetTableSize(tableN), data.GetTableSize(tableN), tpch.Buckets[dp.Region])
			}
			gcChan <- tableN
		} else { //Customer, nation, region, supplier: they never need to wait.
			go makeCRDTUpdatesFromProcTables(0, tableN, 0, data.GetTableSize(tableN), data.GetTableSize(tableN), tpch.Buckets[dp.Region]) //start and end are ignored for these tables
			if tableN == tpch.SUPPLIER {
				procSupp = true
				if waitingPartSupp {
					fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Supplier table has been processed. PartSupp was waiting. Preparing PartSupp.\n")
					go makeCRDTUpdatesFromProcTables(0, tpch.PARTSUPP, 0, data.GetTableSize(tpch.PARTSUPP), data.GetTableSize(tpch.PARTSUPP), tpch.Buckets[dp.Region])
				} else {
					fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Supplier table has been processed. PartSupp is not ready yet.\n")
				}
				if procCust && waitingItems.AnyBitIsSet() { //Usually will not happen as Customer's table is much longer than Supplier's.
					count := 0
					for i := 0; i < nItems; i++ {
						if waitingItems.GetBit(i) {
							go makeCRDTUpdatesFromProcTables(i, tpch.LINEITEM, int(data.SplitFilesStartPos[i].Load()), int(data.SplitFilesStartPos[i+1].Load()), int(data.SplitNLineItems[i].Load()), tpch.Buckets[dp.Region])
							count++
							itemsDone.Set(i)
						}
					}
					fmt.Printf("[TPCH-DL][PrepareCrdtUpdates][WARNING]Supplier table has been processed. Customers were already processed (unexpected, but OK). There are %d/%d waiting items, preparing them.\n", count, nItems)
				}
			} else if tableN == tpch.CUSTOMER {
				procCust = true
				if waitingOrders.AnyBitIsSet() {
					prepareWaitingTablesHelper(waitingOrders, waitingItems, itemsDone, ordersDone, nOrders, nItems, procSupp)
				}
				gcChan <- tableN
			}
		}
		data.CleanTableRawData(tableN) //Free up memory. This memory can safely be freed even if we have to wait before creating the update.
		fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Request %d out of %d done.\n", i, tpch.NTotalFiles-1)
	}
	fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]BitSets.\nItems: \t\t%s.\nOrders: \t%s.\nOrders done: \t%s.\n", waitingItems.ToString(), waitingOrders.ToString(), ordersDone.ToString())
	/*for i := 0; i < nItems; i++ {
		if waitingItems.GetBit(i) {
			fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Requesting lineitem %d/%d to generate updates.\n", i, nItems)
			go makeCRDTUpdatesFromProcTables(i, tpch.LINEITEM, int(data.SplitFilesStartPos[i].Load()), int(data.SplitFilesStartPos[i+1].Load()), int(data.SplitNLineItems[i].Load()), tpch.Buckets[dp.Region])
		} else {
			fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]WARNING!!! Received all tables but somehow, lineitem file %d/%d is not set to generate update!\n", i, nItems)
		}
	}*/
	//The caller of this function is not the one that will listen to updChan. So we are done here
	end := time.Now()
	fmt.Printf("[TPCH-DL]Finished requesting creation of all data protos at %s. Took %d ms. Time taken since DL start: %dms.\n", end.Format("15:04:05.000"), (end.UnixNano()/1000000)-readyStart.UnixNano()/1000000, (end.UnixNano()/1000000)-startTime)
	data.FinishCleanTableRawData()
	//Clean up read2Chan. Read2Chan always finishes before procChan.
	close(data.Read2Chan)
	data.Read2Chan = nil
	//Only at this point we know that we have all the tables created
	if dp.QueryNumbers != nil {
		fmt.Printf("[TPCH-DL]Query numbers is not nil (%v), starting goroutine to load index data.\n", dp.QueryNumbers)
		go LoadIndexData() //TODO: Start loading some views earlier? (i.e., views that do not need all tables)
	} else {
		fmt.Printf("[TPCH-DL][WARNING]Query numbers are nil (%v)! Not starting to load index data.\n", dp.QueryNumbers)
	}
	//Request GC to clean raw data.
	debug.SetGCPercent(100) //Restoring normal GC behaviour.
	runtime.GC()
	ignore(procFullOrders)
}

func prepareWaitingTablesHelper(waitingOrders, waitingItems, itemsDone, ordersDone tools.BitSet, nOrders, nItems int, procSupp bool) {
	//This commented code only works if we assume Suppliers always finish before Customers.
	/*for i := 0; i < nOrders; i++ {
		if waitingOrders.GetBit(i) {
			go preparePartionedTables(i+tpch.N_TABLES, tpch.ORDERS, tpch.Buckets[dp.Region])
			if waitingItems.GetBit(i) {
				go prepareMultiPartionedTables(i+tpch.N_TABLES, tpch.LINEITEM, tpch.Buckets[dp.Region])
			}
		}
	}
	if nOrders == 1 { //Orders not partitioned. Iterate items.
		for i := 0; i < nItems; i++ {
			if waitingItems.GetBit(i) {
				go prepareMultiPartionedTables(i+tpch.N_TABLES, tpch.LINEITEM, tpch.Buckets[dp.Region])
			}
		}
	}*/
	countOrders, countItems := 0, 0
	currOrderStart, currOrderEnd := 0, 0
	for i := 0; i < nOrders; i++ {
		if waitingOrders.GetBit(i) {
			currOrderStart, currOrderEnd = int(data.SplitFilesStartPos[i].Load())+1, int(data.SplitFilesStartPos[i+1].Load())+1
			go makeCRDTUpdatesFromProcTables(i, tpch.ORDERS, currOrderStart, currOrderEnd, currOrderEnd-currOrderStart, tpch.Buckets[dp.Region])
			countOrders++
		}
	}
	if procSupp {
		for i := 0; i < nItems; i++ {
			//Can only start this after the respective order has arrived. Either waitingOrders.GetBit(i) (as then it got started above), or ordersDone.GetBit(i). Otherwise, the items may have arrived before the order.
			if waitingItems.GetBit(i) && (waitingOrders.GetBit(i) || ordersDone.GetBit(i)) {
				go makeCRDTUpdatesFromProcTables(i, tpch.LINEITEM, int(data.SplitFilesStartPos[i].Load()), int(data.SplitFilesStartPos[i+1].Load()), int(data.SplitNLineItems[i].Load()), tpch.Buckets[dp.Region])
				countItems++
				itemsDone.Set(i)
			}
		}
		//fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Customer table has been processed. There are %d/%d waiting orders and %d/%d waiting items, preparing them.\n", countOrders, nOrders, countItems, nItems)
	} /*else {
		fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Customer table has been processed. There are %d/%d waiting orders, preparing them. There are some waiting items, but suppliers not yet processed, so not preparing them.\n", countOrders, nOrders)
	}*/
}

func PrepareCrdtUpdates() {
	fmt.Println("[TPCH-DL]Starting to prepare updates... (raw data version)")
	//Need 3 types:
	//Replicated everywhere (parts). Each server processes a part of it (pun intended).
	//Replicated locally (order, supplier, itemsupply, probably some other I'm forgetting)
	//Replicated locally + one other place (lineitem)/*
	regionBkt, leftToPrepare, nProcReceived := tpch.Buckets[dp.Region], tpch.NTotalFiles, 0
	//Requirements (of processed tables) for each table that we want to make updates for:
	//cust: nation, region
	//lineitem: supplier, order, customer, nation
	//nation: region
	//orders: customer, nation
	//part: N/A
	//partsupp: supplier, nation
	//region: N/A
	//supplier: nation
	//Note: Region and Nation will ALWAYS be the first two (by this order) to come out of procChan. So we can use this to our advantage.

	//Wait for data.ProcChan; process Region (1st one)
	tableN := <-data.ProcChan
	start := time.Now().UnixNano() / 1000000
	go preparePartionedTables(tableN, tableN, regionBkt)
	//Wait for data.ProcChan; process Nation (2nd one)
	tableN = <-data.ProcChan
	go preparePartionedTables(tableN, tableN, regionBkt)
	//Take out the first two entries of data.Read2Chan (Region, Nation)
	<-data.Read2Chan
	<-data.Read2Chan
	cleanChan <- tpch.REGION
	cleanChan <- tpch.NATION
	leftToPrepare -= 2
	nProcReceived += 2
	var procCust, procSupp, procOrder bool
	var waitingPartSupp bool
	nOrderFiles, nItemFiles := data.GetOrderFilesCount(), data.GetLineItemFilesCount()
	waitingLineItems, waitingOrders := make([]int, 0, nOrderFiles), make([]int, 0, nItemFiles)
	readyStart := time.Now()
	fmt.Printf("[TPCH-DL]Ready to prepare CRDT updates at %s. Time since start: %dms.\n", readyStart.Format("15:04:05.000"), (readyStart.UnixNano()/1000000)-startTime)
	for leftToPrepare > 0 {
		fmt.Printf("[TPCH-DL]Waiting for procChan or read2Chan. Len of proc Chan: %d. Len of read2Chan: %d. Left to prepare: %d.\n", len(data.ProcChan), len(data.Read2Chan), leftToPrepare)
		select {
		case tableN = <-data.ProcChan:
			fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Got table number %d from data.ProcChan.\n", tableN)
			switch tableN {
			case tpch.CUSTOMER:
				fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Customer table has been processed. Are orders waiting: %v.\n", waitingOrders)
				procCust = true
				if len(waitingOrders) > 0 {
					for _, tableN := range waitingOrders {
						go preparePartionedTables(tableN, tpch.ORDERS, regionBkt)
					}
					leftToPrepare -= len(waitingOrders)
					waitingOrders = nil //Reset
				}
				if len(waitingLineItems) > 0 && nOrderFiles > 1 { //In this case, orders and items are like a pair. Process the waiting lineitems too.
					for _, tableN := range waitingLineItems {
						go prepareMultiPartionedTables(tableN, tpch.LINEITEM, regionBkt)
					}
					leftToPrepare -= len(waitingLineItems)
					waitingLineItems = nil //Reset
				}
			case tpch.SUPPLIER:
				fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Supplier table has been processed. Are partSupp waiting: %v.\n", waitingPartSupp)
				procSupp = true
				if waitingPartSupp {
					go preparePartionedTables(tpch.PARTSUPP, tpch.PARTSUPP, regionBkt)
					leftToPrepare--
				}
			case tpch.ORDERS:
				fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Orders table has been processed. Are lineitems waiting (yes if above 0): %v.\n", len(waitingLineItems))
				procOrder = true
				if len(waitingLineItems) > 0 {
					for _, tableN := range waitingLineItems {
						go prepareMultiPartionedTables(tableN, tpch.LINEITEM, regionBkt)
					}
					leftToPrepare -= len(waitingLineItems)
					waitingLineItems = nil //Reset
				}
			default:
				fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Table %s (%d) has been processed, but it is not one of the ones we are waiting for.\n", tpch.GetTableName(tableN), tpch.GetSplitOffset(tableN))
			}
			nProcReceived++
			cleanChan <- tableN
		case tableN = <-data.Read2Chan:
			fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Got table number %d from data.Read2Chan.\n", tableN)
			leftToPrepare--
			switch tableN {
			case tpch.SUPPLIER, tpch.CUSTOMER:
				go preparePartionedTables(tableN, tableN, regionBkt)
			case tpch.PART:
				go prepareTableEverywhere(tableN, tpch.Buckets[PART_BKT_INDEX])
			case tpch.PARTSUPP:
				if procSupp {
					go preparePartionedTables(tableN, tableN, regionBkt)
				} else {
					fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Queuing partSupp, as supplier table has not yet been processed.\n")
					waitingPartSupp, leftToPrepare = true, leftToPrepare+1
				}
			/*case tpch.ORDERS:
				if procCust {
					go preparePartionedTables(tableN, tableN, regionBkt)
				} else {
					fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Queuing orders, as customers table has not yet been processed.\n")
					waitingOrders, leftToPrepare = true, leftToPrepare+1
				}
			default: //Lineitem, whenever it is tpch.LINEITEM or part of lineitem.
				if procCust && procSupp && procOrder {
					go prepareMultiPartionedTables(tableN, tpch.LINEITEM, regionBkt)
				} else {
					fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Queuing lineitems, as either customers, suppliers or orders tables have not yet been processed.\n")
					waitingLineItems, leftToPrepare = append(waitingLineItems, tableN), leftToPrepare+1 //Add to waiting list
				}
			}*/
			default: //Could be either orders or lineitems, split or not. We only need to distinguish between order and lineitem.
				if tableN == tpch.ORDERS || (nOrderFiles > 1 && tpch.N_TABLES < tableN && tableN < tpch.NTotalFiles+nOrderFiles) { //Order, not split (first) or split (second)
					if procCust {
						go preparePartionedTables(tableN, tpch.ORDERS, regionBkt)
					} else {
						fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Queuing orders, as customers table has not yet been processed.\n")
						waitingOrders, leftToPrepare = append(waitingOrders, tableN), leftToPrepare+1 //Add to waiting list.
					}
				} else { //Has to be lineitem
					if !procCust || !procSupp || (nOrderFiles == 1 && !procOrder) { //If both orders and lineitems are split, then the respective order will ALWAYS come first.
						fmt.Printf("[TPCH-DL][PrepareCrdtUpdates]Queuing lineitems, as either customers, suppliers or orders tables have not yet been processed.\n")
						waitingLineItems, leftToPrepare = append(waitingLineItems, tableN), leftToPrepare+1 //Add to waiting list
					} else {
						go prepareMultiPartionedTables(tableN, tpch.LINEITEM, regionBkt)
					}
				}
			}
		}
	}
	//The caller of this function is the one that will listen to updChan. So we are done here
	end := time.Now()
	fmt.Printf("[TPCH-DL]Finished requesting creation of all data protos at %s. Took %d ms. Time taken since DL start: %dms.\n", end.Format("15:04:05.000"), (end.UnixNano()/1000000)-start, (end.UnixNano()/1000000)-startTime)
	//Wait for remaining procChan, to clean up + load index data. Note: procChan will ALWAYS finish after read2Chan.
	for ; nProcReceived < tpch.NTotalFiles; nProcReceived++ {
		cleanChan <- <-data.ProcChan
	}
	//Only at this point we know that we have all the tables created
	if dp.QueryNumbers != nil {
		//time.Sleep(5000 * time.Millisecond)
		go LoadIndexData() //TODO: Start loading views that do not need LineItems sooner?
	}
}

/*func PrepareCrdtUpdates() {
	fmt.Println("[TPCH-DL]Starting to prepare updates...")
	//Need 3 types:
	//Replicated everywhere (parts). Each server processes a part of it (pun intended).
	//Replicated locally (order, supplier, itemsupply, probably some other I'm forgetting)
	//Replicated locally + one other place (lineitem)
	nTables, regionBkt := len(tpch.TableNames), tpch.Buckets[dp.Region]
	nProcess := nTables - 1 + data.GetLineItemFilesCount()

	start := int64(0)
	for i := 0; i < nProcess; i++ {
		tableN := <-data.ProcChan
		if start == 0 {
			start = time.Now().UnixNano() / 1000000
		}
		var name string
		if tableN > len(tpch.TableNames) {
			name = tpch.TableNames[tpch.LINEITEM] + "_" + strconv.Itoa(tableN-tpch.N_TABLES)
		} else {
			name = tpch.TableNames[tableN]
		}
		//fmt.Println("[TPCH-DL]Preparing protos", name, tableN)
		if tableN == tpch.PART {
			go prepareTableEverywhere(tableN, tpch.Buckets[PART_BKT_INDEX])
		} else if tableN == tpch.LINEITEM || tableN > tpch.N_TABLES { //2nd case: Lineitem table is split.
			go prepareMultiPartionedTables(tableN, tpch.LINEITEM, regionBkt)
		} else {
			go preparePartionedTables(tableN, regionBkt)
		}
	}
	//Only at this point we know that we have all the tables created
	if dp.QueryNumbers != nil {
		//time.Sleep(5000 * time.Millisecond)
		go LoadIndexData() //TODO: Start loading views that do not need LineItems sooner?
	}
	//The caller of this function is the one that will listen to updChan. So we are done here
	end := time.Now()
	fmt.Printf("[TPCH-DL]Finished requesting creation of all data protos at %s. Took %d ms. Time taken since DL start: %dms.\n", end.Format("15:04:05.000"), (end.UnixNano()/1000000)-start, (end.UnixNano()/1000000)-startTime)
}*/

func SF1PrepareBaseDataUpdsAndSend() {
	fmt.Println("[TPCH-DL]Starting to prepare updates... (1SF version)")
	upds := SF1PrepareCrdtUpdates()
	fmt.Println("[TPCH-DL]Starting to send base data...")
	SF1SendBaseData(upds)
	//fmt.Println("[TPCH-DL]IsGlobal:", dp.IndexConfigs.IsGlobal)
	/*if dp.IndexConfigs.IsGlobal {
		go SendIndexData()
	} else {
		go SendLocalIndexData()
	}*/
	data.CleanAll()
	confs := data.TpchConfigs
	data = nil
	data = &tpch.TpchData{TpchConfigs: confs}
}

func SF1PrepareCrdtUpdates() []crdt.UpdateObjectParams { //TODO: Might want to optimize this to start sending txns individually to TM, as now we're loading huge datasets.
	//Need 3 types:
	//Replicated everywhere (parts). Each server processes a part of it (pun intended).
	//Replicated locally (order, supplier, itemsupply, probably some other I'm forgetting)
	//Replicated locally + one other place (lineitem)
	nTables, regionBkt := len(tpch.TableNames), tpch.Buckets[dp.Region] //Not considering lineitems yet
	nUpds := nTables - 1 + getLineitemsNRoutines()
	updChan = make(chan crdt.UpdateObjectParams, nUpds)

	start := int64(0)
	for i := 0; i < nTables; i++ {
		//fmt.Printf("[TPCH-DL]NTables: %d. Size of channel: %d.\n", nTables, cap(data.ProcChan))
		tableN := <-data.ProcChan
		if start == 0 {
			start = time.Now().UnixNano() / 1000000
			//if data.AreLineitemsSplit() { //Send initializer to RWEmbMap.
			//TODO: Figure out what is the actual size, as the lineitems are spread across regions.
			//To figure this out we need to already have most of the data processed (at least orders + customers.)
			//Will probably need to refactor the workflow quite a bit, as ideally we want this before we finish preparing all updates.
			//I should test if processing the tables ends much before preparing the protobufs... if it does, can use that.
			//When we read the orders, we can find out where they belong to... is this enough?
			//We can then make an int8[] array to indicate the region of each order (in theory could be some some hacky int4[] or int3[])
			//With this we immediatelly know how many orders for each region, but we still do not have the exact number of items.
			//Maybe this is all not needed, or an estimation like this is already ok - the map does not allocate an exact amount of slots.
			//So having a bit over is likely ok.
			//}
		}
		//fmt.Println("[TPCH-DL]Preparing protos", tpch.TableNames[tableN], tableN)
		if tableN == tpch.PART {
			go prepareTableEverywhere(tableN, tpch.Buckets[PART_BKT_INDEX])
		} else if tableN == tpch.LINEITEM {
			go prepareMultiPartionedTables(tpch.LINEITEM, tpch.LINEITEM, regionBkt)
		} else {
			go preparePartionedTables(tableN, tableN, regionBkt)
		}
	}
	//Only at this point we know that we have all the tables created
	if dp.QueryNumbers != nil {
		//time.Sleep(5000 * time.Millisecond)
		go LoadIndexData()
	}
	//nUpds-- //TODO: REMOVE THIS!!!
	upds := make([]crdt.UpdateObjectParams, nUpds)
	for i := 0; i < nUpds; i++ {
		upds[i] = <-updChan
	}
	end := time.Now()
	fmt.Printf("[TPCH-DL]Finished creating all data protos at %s. Took %dms. Time taken since DL start: %dms.\n", end.Format("15:04:05.000"), (end.UnixNano()/1000000)-start, (end.UnixNano()/1000000)-startTime)
	return upds
}

func SF1SendBaseData(upds []crdt.UpdateObjectParams) {
	fmt.Printf("[TPCH-DL]All protobufs created, waiting for TM at %s.\n", time.Now().Format("15:04:05.000"))
	<-dp.IsTMReady //Wait until TM is ready
	fmt.Println("[TPCH-DL]TM ready at: ", time.Now().Format("15:04:05.000"))
	//time.Sleep(1000 * time.Millisecond)
	start := time.Now().UnixNano()
	confirmChan := make(chan bool, len(upds))
	for _, upd := range upds {
		go updateHelper(upd, confirmChan)
	}
	for i := 0; i < len(upds); i++ {
		<-confirmChan
	}
	//ic = InternalClient{}.Initialize(dp.Tm)
	//ic.DoUpdate(upds)
	end := time.Now()
	fmt.Printf("[TPCH-DL]Done, TM commited at %s. Time taken: %dms\n", end.Format("15:04:05.000"), (end.UnixNano()-start)/int64(time.Millisecond))
	//debugRead(ic)
}

func updateHelperNoConfirmation(upd crdt.UpdateObjectParams) {
	newC := InternalClient{}.Initialize(dp.Tm)
	newC.DoSingleUpdate(upd.KeyParams, upd.UpdateArgs)
	newC.CloseClient()
}

func updateHelper(upd crdt.UpdateObjectParams, confirmChan chan bool) {
	newC := InternalClient{}.Initialize(dp.Tm)
	newC.DoSingleUpdate(upd.KeyParams, upd.UpdateArgs)
	confirmChan <- true
	newC.CloseClient()
}

func multiUpdHelper(upds []crdt.UpdateObjectParams, confirmChan chan bool) {
	newC := InternalClient{}.Initialize(dp.Tm)
	newC.DoUpdate(upds)
	confirmChan <- true
	newC.CloseClient()
}

func initDataHelperNoConfirmation(upds []crdt.UpdateObjectParams) {
	newC := InternalClient{}.Initialize(dp.Tm)
	newC.DoNonBlockingInitialDataUpdate(upds)
	newC.CloseClient()
}

func initDataNonBlockingHelper(upds []crdt.UpdateObjectParams, confirmChan chan bool) {
	newC := InternalClient{}.Initialize(dp.Tm)
	newC.DoInitialDataUpdate(upds)
	confirmChan <- true
	newC.CloseClient()
}

func requestPotionDBGC() {
	newC := InternalClient{}.Initialize(dp.Tm)
	newC.RequestManualGC()
	newC.CloseClient()
}

func debugRead(tc InternalClient) {
	/*
		tableRead := ReadObjectParams{
			KeyParams: KeyParams{Key: tpch.TableNames[tpch.CUSTOMER], CrdtType: proto.CRDTType_RRMAP, Bucket: tpch.Buckets[dp.Region]},
			ReadArgs:  crdt.EmbMapPartialArguments{Args: crdt.GetValueArguments{Key: }},
		}
	*/
	fmt.Println("Sleeping for 15s before reading...")
	time.Sleep(15 * time.Second)
	tableRead := crdt.ReadObjectParams{
		KeyParams: crdt.MakeKeyParams(tpch.TableNames[tpch.SUPPLIER], proto.CRDTType_RRMAP, tpch.Buckets[dp.Region]),
		ReadArgs:  crdt.StateReadArguments{},
	}
	state := tc.DoRead([]crdt.ReadObjectParams{tableRead})
	fmt.Printf("Read result: %T %v\n", state[0], state[0])
}

// dataOffset: position in RawTables (to account for partitioned tables)
func prepareMultiPartionedTables(dataOffset int, tableI int, bucket string) {
	table, toRead, keys, header, name, regFunc := data.RawTables[dataOffset], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI], data.Tables.LineitemSliceToRegion

	fmt.Printf("[TPCH-DL]Preparing CRDT updates for table %s (TableN %d, offset %d) for region %d\n", name, tableI, dataOffset, dp.Region)
	//nRoutines := len(table)/7000000 + 1
	//nRoutines := len(table)/1510000 + 1
	nRoutines := getLineitemsNRoutines()
	//fmt.Printf("[TPCH-DL]Using %d goroutines to prepare lineitem data\n", nRoutines)
	//nRoutines := 1
	if nRoutines == 1 {
		//No extra goroutine
		multiPartionedTableHelper(table, toRead, keys, header, name, bucket, regFunc, make(chan bool, 1)) //We will ignore this channel
		if cleanChan != nil {
			cleanChan <- tableI
		}
		return
	}
	factor := len(table) / nRoutines
	startI, endI, endChan := 0, factor, make(chan bool, nRoutines)
	for i := 0; i < nRoutines; i++ {
		slicedTable := table[startI:endI]
		go multiPartionedTableHelper(slicedTable, toRead, keys, header, name, bucket, regFunc, endChan)
		startI = endI
		endI += factor
		if i == nRoutines-2 {
			endI = len(table)
		}
	}
	if cleanChan != nil { //Wait for replies in order to notify clean routine
		for i := 0; i < nRoutines; i++ {
			<-endChan
		}
		cleanChan <- tableI
	}
}

// Note: For a single, 10SF, lineitem table, this takes ~35s in Paradoxe. The problem is that it's hard to partition this. (Unless we make multiple buffers)
// But maybe OK as we intend to split into multiple files.
// For lineitems with two regions, each server only instanciates the version corresponding to its region.
func multiPartionedTableHelper(table [][]string, toRead []int8, keys []int, header []string, name string, bucket string,
	regFunc func(obj []string, buf []int8), endChan chan bool) { //, endChan chan *UpdateObjectParams) {

	start := time.Now().UnixNano()
	var embMapUpd []crdt.EmbMapUpdate
	if dp.Sf >= 10 {
		embMapUpd = make([]crdt.EmbMapUpdate, int(float64(len(table))*0.35))
	} else if dp.Sf >= 0.1 {
		embMapUpd = make([]crdt.EmbMapUpdate, int(float64(len(table))*0.4))
	} else {
		embMapUpd = make([]crdt.EmbMapUpdate, int(float64(len(table))))
	}
	i := 0
	//embMapUpd, i := make([]crdt.EmbMapUpdate, len(table)*2), 0
	regs := make([]int8, 2)
	for _, obj := range table {
		regFunc(obj, regs)
		if regs[0] == dp.Region || (len(regs) == 2 && regs[1] == dp.Region) {
			//If one of the regions is of this server, create the item
			//key, innerUpd := GetInnerMapEntryArray(header, keys, obj, toRead) //Update for one entry in the map
			//key, innerUpd := GetInnerMapEntryCompactArray(header, keys, obj, toRead) //Update for one entry in the map
			key, innerUpd := GetInnerMapEntryStringArray(header, keys, obj, toRead) //Update for one entry in the map
			embMapUpd[i] = crdt.EmbMapUpdate{Key: key, Upd: innerUpd}
			i++
		}
	}
	regs = nil
	end := time.Now().UnixNano()
	fmt.Printf("[TPCH-DL]Time taken to prepare lineitem data: %dms\n", (end-start)/int64(time.Millisecond))
	updChan <- crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapUpdateAllArray{Upds: embMapUpd[:i]}}
	if cleanChan != nil {
		endChan <- true
	}
}

// func preparePartionedTables(tableI int, bucket string) *UpdateObjectParams {
// dataOffset: position in RawTables (to account for partitioned tables)
func preparePartionedTables(dataOffset, tableI int, bucket string) {
	table, toRead, keys, header, name, regFunc := data.RawTables[dataOffset], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI], regionFuncs[tableI]
	fmt.Printf("[TPCH-DL]Preparing CRDT updates for table %s (%d) for region %d\n", name, tableI, dp.Region)
	var embMapUpd []crdt.EmbMapUpdate
	if dp.Sf > 1 && (tableI == tpch.CUSTOMER || tableI == tpch.ORDERS || tableI == tpch.PARTSUPP) {
		embMapUpd = make([]crdt.EmbMapUpdate, int(float64(len(table))*0.21))
	} else if dp.Sf >= 0.1 {
		embMapUpd = make([]crdt.EmbMapUpdate, len(table)/4)
	} else {
		embMapUpd = make([]crdt.EmbMapUpdate, len(table)/3)
	}
	i := 0
	//embMapUpd, i := make([]crdt.EmbMapUpdate, len(table)), 0
	for _, obj := range table {
		if regFunc(obj) == dp.Region {
			//key, innerUpd := GetInnerMapEntryArray(header, keys, obj, toRead) //Update for one entry in the map
			//key, innerUpd := GetInnerMapEntryCompactArray(header, keys, obj, toRead) //Update for one entry in the map
			key, innerUpd := GetInnerMapEntryStringArray(header, keys, obj, toRead) //Update for one entry in the map
			embMapUpd[i] = crdt.EmbMapUpdate{Key: key, Upd: innerUpd}
			i++
		}
		//Ignore if it's not of the region of this server
		//ignore(regFunc)
	}
	updChan <- crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapUpdateAllArray{Upds: embMapUpd[:i]}}
	if cleanChan != nil {
		cleanChan <- tableI
	}
}

// For tables that are to be replicated in every server and thus need no filtering
// For this type of tables (PART), each server loads a portion of the data, based on its region and number of regions.
// func prepareTableEverywhere(tableI int, bucket string) *UpdateObjectParams {
func prepareTableEverywhere(tableI int, bucket string) {
	table, toRead, keys, header, name := data.RawTables[tableI], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI]
	fmt.Printf("[TPCH-DL]Preparing CRDT updates for table %s (%d) for region %d\n", name, tableI, dp.Region)
	nRegions, myRegion, tableLen := len(data.Tables.Regions), int(dp.Region), len(table)
	startI, finishI := (tableLen/nRegions)*myRegion, (tableLen/nRegions)*(myRegion+1)
	//startI, finishI := 0, tableLen
	embMapUpd, i := make([]crdt.EmbMapUpdate, len(table)/5), 0
	//embMapUpd, i := make([]crdt.EmbMapUpdate, len(table)), 0
	table = table[startI:finishI]
	for _, obj := range table {
		//key, innerUpd := GetInnerMapEntryArray(header, keys, obj, toRead) //Update for one entry in the map
		//key, innerUpd := GetInnerMapEntryCompactArray(header, keys, obj, toRead) //Update for one entry in the map
		key, innerUpd := GetInnerMapEntryStringArray(header, keys, obj, toRead) //Update for one entry in the map
		embMapUpd[i] = crdt.EmbMapUpdate{Key: key, Upd: innerUpd}
		i++
	}
	updChan <- crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapUpdateAllArray{Upds: embMapUpd[:i]}}
	ignore(nRegions, myRegion)
	if cleanChan != nil {
		cleanChan <- tableI
	}
}

// Note: This function does not call multiple routines for lineitems, as it is assumed that lineitems are already split into multiple files.
// Start is inclusive, end is exclusive.
// Note2: offset is merely informative for printing purposes.
func makeCRDTUpdatesFromProcTables(offset, tableI, start, end, tableLength int, bucket string) {
	//fmt.Printf("[TPCH-DL]Preparing CRDT updates for table %s (%d) for region %d\n", tpch.TableNames[tableI], tableI, dp.Region)
	var embMapUpd []crdt.EmbMapUpdate
	//fmt.Printf("[TPCH-DL]Preparing CRDT updates for table %s (%d_%d) for region %d. Range: [%d:%d[. Length: %d\n", tpch.TableNames[tableI],
	//	tableI, offset, dp.Region, start, end, tableLength)
	startTs := time.Now().UnixNano()
	nLineItemsCount := data.GetLineItemFilesCount()
	switch tableI {
	case tpch.LINEITEM:
		if dp.Sf >= 10 && nLineItemsCount == 1 { //If lineitems are split, then it's as if Sf was smaller.
			embMapUpd = make([]crdt.EmbMapUpdate, int(float64(tableLength)*0.35)) //This can be reduced after we make items more local.
		} else if dp.Sf >= 1 {
			embMapUpd = make([]crdt.EmbMapUpdate, int(float64(tableLength)*0.38)) //This can be reduced after we make items more local.
		} else if dp.Sf >= 0.1 {
			embMapUpd = make([]crdt.EmbMapUpdate, int(float64(tableLength)*0.4))
		} else {
			embMapUpd = make([]crdt.EmbMapUpdate, tableLength)
		}
	case tpch.PART:
		embMapUpd = make([]crdt.EmbMapUpdate, tableLength/5)
	default:
		if dp.Sf > 1 && (tableI == tpch.CUSTOMER || tableI == tpch.ORDERS || tableI == tpch.PARTSUPP) {
			embMapUpd = make([]crdt.EmbMapUpdate, int(float64(tableLength)*0.21))
		} else if dp.Sf >= 0.1 {
			embMapUpd = make([]crdt.EmbMapUpdate, tableLength/4)
		} else {
			embMapUpd = make([]crdt.EmbMapUpdate, tableLength/3)
		}
	}
	written := 0
	var key string
	var currUpd crdt.SetValue

	switch tableI {
	case tpch.LINEITEM:
		//var regionsPair int8
		var orderReg, suppReg int8
		var items [][]tpch.LineItem
		if nLineItemsCount == 1 { //Not split lineitems
			items = data.Tables.LineItems
		} else { //Split lineitems
			items = data.Tables.LineItems[start:end]
		}
		if len(items) == 0 {
			fmt.Printf("[TPCH-DL][CRDTUpdates]ERROR: lineitems is nil. nLineItemsCount: %d. Offset: %d. Start: %d. End: %d. Table length: %d\n", nLineItemsCount, offset, start, end, tableLength)
			fmt.Print(items[0]) //Forcing a crash
		} else if items[0] == nil {
			nNil := 0
			for i := 0; i < tools.Min(100, len(items)); i++ {
				if items[i] == nil {
					nNil++
				}
			}
			fmt.Printf("[TPCH-DL][CRDTUpdates]WARNING: First item in lineitems is nil. Number of nil entries (only counting first 100): %v. Offset: %d. Start: %d. End: %d. Table length: %d\n", nNil, offset, start, end, tableLength)
		} /*else {
			fmt.Printf("[TPCH-DL][CRDTUpdates]First item in lineitems is not nil. Offset: %d. Start: %d. End: %d. Table length: %d\n", offset, start, end, tableLength)
		}*/
		minOrderId := items[0][0].L_ORDERKEY
		minLen, testId := int32(0), int32(1)
		for testId <= minOrderId { //Counting digits
			minLen++
			testId *= 10
		}
		currOrderID, previousOrderID := int32(0), int32(0)
		var byteBuf = make([]byte, minLen+2)                    //+2: '_' and linenumber (1-8)
		strconv.AppendInt(byteBuf[:0], int64(minOrderId-1), 10) //We'll need this as UpdateIncreasingNumberStringBuf can't handle empty bufs or situations with no increment.
		//Idea of byteBuf: we will only write the key once per order. The '_' is only rewritten when the next orderID is longer than the previous one.
		//So, for each item, the orderID is only written once, and then we only need to update the last byte in byteBuf to reflect the LINENUMBER.
		//string(byteBuf) will create a copy of the bytes, so we can reuse byteBuf for the next step.
		const oneChar = byte('0')
		for _, orderItems := range items {
			if len(orderItems) == 0 {
				fmt.Printf("[TPCH-DL][CRDTUpdates]WARNING: Found empty orderItems in lineitems. Offset: %d. Start: %d. End: %d. PreviousOrderID: %d. Table length: %d\n", offset, start, end, previousOrderID, tableLength)
			}
			currOrderID = orderItems[0].L_ORDERKEY
			orderReg = data.Tables.OrderkeyToRegionkey(currOrderID)
			if orderReg == dp.Region { //Can do directly without further checks
				//fmt.Printf("[TPCH-DL][CRDTUpdates][ORDER]MinLen: %d. TestId: %d. Current orderID: %d. Previous orderID: %d\n", minLen, testId, currOrderID, previousOrderID)
				if testId <= currOrderID { //Need one more digit
					minLen++
					testId *= 10
					byteBuf = make([]byte, minLen+2) //+2: '_' and linenumber (1-8)
					byteBuf[minLen] = '_'
					strconv.AppendInt(byteBuf[:0], int64(currOrderID), 10) //Write orderkey
				} else {
					UpdateIncreasingNumberStringBuf(previousOrderID, currOrderID, byteBuf[:minLen])
				}
				//strconv.AppendInt(byteBuf[:0], int64(orderItems[0].L_ORDERKEY), 10) //Write orderkey
				//byteBuf = byteBuf[:minLen+2]
				byteBuf[minLen+1] = '1' //First item is 1 and it is always increasing.
				for _, lineitem := range orderItems {
					//byteBuf[minLen+1] = oneChar + byte(lineitem.L_LINENUMBER)
					embMapUpd[written] = crdt.EmbMapUpdate{Key: string(byteBuf), Upd: crdt.SetValue{NewValue: lineitem.ToBytes()}} //string(byteBuf) does a copy
					written++
					byteBuf[minLen+1]++ //Next item will be +1
				}
				previousOrderID = currOrderID
			} else { //Have to keep checking. We know orderID is not our region, so we only need to check supplier's.
				hasWrittenKey := false
				for _, lineitem := range orderItems {
					suppReg = data.Tables.SuppkeyToRegionkey(int64(lineitem.L_SUPPKEY))
					if suppReg == dp.Region {
						//fmt.Printf("[TPCH-DL][CRDTUpdates][Supp]MinLen: %d. TestId: %d. Current orderID: %d. Previous orderID: %d\n", minLen, testId, lineitem.L_ORDERKEY, previousOrderID)
						if !hasWrittenKey { //Write key only once
							if testId <= lineitem.L_ORDERKEY { //Need one more digit
								minLen++
								testId *= 10
								byteBuf = make([]byte, minLen+2) //+2: '_' and linenumber (1-8)
								byteBuf[minLen] = '_'
								strconv.AppendInt(byteBuf[:0], int64(lineitem.L_ORDERKEY), 10) //Write orderKey
							} else {
								UpdateIncreasingNumberStringBuf(previousOrderID, lineitem.L_ORDERKEY, byteBuf[:minLen])
							}
							//strconv.AppendInt(byteBuf[:0], int64(lineitem.L_ORDERKEY), 10) //Write orderKey
							//byteBuf = byteBuf[:minLen+2]
							hasWrittenKey = true
							previousOrderID = lineitem.L_ORDERKEY
						}
						byteBuf[minLen+1] = oneChar + byte(lineitem.L_LINENUMBER)
						embMapUpd[written] = crdt.EmbMapUpdate{Key: string(byteBuf), Upd: crdt.SetValue{NewValue: lineitem.ToBytes()}} //string(byteBuf) does a copy
						written++
					}
				}
			}
		}

		/*for i, orderItems := range items {
			for _, lineitem := range orderItems {
				regionsPair = data.Tables.GetLineItemRegions(lineitem)
				if regionsPair/tpch.LINEITEM_REGION_EXTRACT_FACTOR == dp.Region || (regionsPair%tpch.LINEITEM_REGION_EXTRACT_FACTOR) == dp.Region {
					key, currUpd = lineitem.GetPrimaryKey(), crdt.SetValue{NewValue: lineitem.ToBytes()}
					if written >= len(embMapUpd) {
						fmt.Printf("[TPCH-DL][CRDTUpdates]WARNING: Writting embMapUpd out of bounds for lineitems. Size: %d. Written: %d. Table length: %d. Order: %d out of %d. Conservative size: %d. Less conservative size: %d. Estimated left: %d\n",
							len(embMapUpd), written, tableLength, i, len(items), int(float64(tableLength)*0.4), int(float64(tableLength)*0.35), int(float64((len(items)-i)*4)*0.4))
					}
					embMapUpd[written] = crdt.EmbMapUpdate{Key: key, Upd: currUpd}
					written++
				}
			}
		}*/
		//Problem: not only we don't know how many items we have, we also don't know how many will we *actually* write.
		//We would need to calculate the regions of each item first... and later recalculate... or use some buffers to hold which items.
		//The worst part is, that for some orders, we only need some items, not all.
		//We also don't want to use multiple crdt.EmbMapUpdate, as that would reduce efficiency when applying the updates.
		//We also have to be careful with copies, because we are lacking memory.
		//I suspect both the key creation and GetLineItemRegions are where most of the cost are. GetLineItemRegions accesses 5 slices to get both order and supplier regions.
		//The only solution I can think of is to have one routine going through the whole array, calculating the regions and setting a bit if it is our region.
		//Then, each "x" steps, it sends the ranges to another routine that will create the actual updates.
		//Potentially, I could split this first part into multiple routines, and then merge the bitsets and starting points.
		//Then, start dispatching routines that will actually make the updates and assign to the slice.
		//Idea: I can reduce the number of accesses by obtaining directly the customer's region.
		//For now, let's keep a single goroutine approach, as maybe the now more efficient code will suffice.
	case tpch.ORDERS:
		var orders []tpch.Orders
		if data.GetOrderFilesCount() == 1 { //Not split orders
			orders = data.Tables.Orders[1:]
		} else { //Split orders
			orders = data.Tables.Orders[start:end]
		}
		for _, order := range orders {
			/*if len(data.Tables.Orders) == 0 || len(data.Tables.Customers) == 0 || len(data.Tables.Suppliers) == 0 {
				fmt.Printf("[TPCH-DL][CRDTUpdates]WARNING: Nil tables detected while making orders for offset %d. Order %d/%d. Orders: %d. Customers: %d. Suppliers: %d\n",
					offset, k, len(orders), len(data.Tables.Orders), len(data.Tables.Customers), len(data.Tables.Suppliers))
			}*/
			if data.Tables.Custkey32ToRegionkey(order.O_CUSTKEY) == dp.Region {
				key, currUpd = order.GetPrimaryKey(), crdt.SetValue{NewValue: order.ToBytes()}
				embMapUpd[written] = crdt.EmbMapUpdate{Key: key, Upd: currUpd}
				written++
			}
		}
	case tpch.CUSTOMER:
		//dataSize, keySize := 0, 0
		//var buf []byte
		for _, cust := range data.Tables.Customers[1:] {
			if data.Tables.Custkey32ToRegionkey(cust.C_CUSTKEY) == dp.Region {
				//buf = cust.ToBytes()
				//key, currUpd = cust.GetPrimaryKey(), crdt.SetValue{NewValue: buf}
				key, currUpd = cust.GetPrimaryKey(), crdt.SetValue{NewValue: cust.ToBytes()}
				embMapUpd[written] = crdt.EmbMapUpdate{Key: key, Upd: currUpd}
				written++
				//dataSize += len(buf)
				//keySize += len(key) + 16 //+16 for the String header overhead
			}
		}
		//fmt.Printf("[TPCH-DL][Customer]Prepared %d out of %d customers for region %d. Bytes size %dMB, key size %dMB\n", written, len(data.Tables.Customers)-1, dp.Region, dataSize/1024/1024, keySize/1024/1024)
	case tpch.SUPPLIER:
		for _, supplier := range data.Tables.Suppliers[1:] {
			if data.Tables.SuppkeyToRegionkey(int64(supplier.S_SUPPKEY)) == dp.Region {
				key, currUpd = supplier.GetPrimaryKey(), crdt.SetValue{NewValue: supplier.ToBytes()}
				embMapUpd[written] = crdt.EmbMapUpdate{Key: key, Upd: currUpd}
				written++
			}
		}
	case tpch.PART: //Each server loads a portion of the data, based on its region and number of regions.
		nRegions, myRegion := len(data.Tables.Regions), int(dp.Region)
		slicedParts := data.Tables.Parts[myRegion*tableLength/nRegions+1 : (myRegion+1)*tableLength/nRegions+1] //+1 as Part 0 is empty
		fmt.Printf("[TPCH-DL]Preparing CRDT updates for PART table. My region: %d. Buf size: %d. Full table length: %d. My part length: %d. Start, end: [%d:%d[.\n",
			myRegion, len(embMapUpd), tableLength, len(slicedParts), myRegion*tableLength/nRegions, (myRegion+1)*tableLength/nRegions)
		for _, part := range slicedParts {
			key, currUpd = part.GetPrimaryKey(), crdt.SetValue{NewValue: part.ToBytes()}
			embMapUpd[written] = crdt.EmbMapUpdate{Key: key, Upd: currUpd}
			written++
		}
	case tpch.PARTSUPP:
		for _, partsupp := range data.Tables.PartSupps {
			if data.Tables.SuppkeyToRegionkey(int64(partsupp.PS_SUPPKEY)) == dp.Region {
				key, currUpd = partsupp.GetPrimaryKey(), crdt.SetValue{NewValue: partsupp.ToBytes()}
				embMapUpd[written] = crdt.EmbMapUpdate{Key: key, Upd: currUpd}
				written++
			}
		}
	case tpch.NATION:
		for _, nation := range data.Tables.Nations {
			if data.Tables.NationkeyToRegionkey(int64(nation.N_NATIONKEY)) == dp.Region {
				key, currUpd = nation.GetPrimaryKey(), crdt.SetValue{NewValue: nation.ToBytes()}
				embMapUpd[written] = crdt.EmbMapUpdate{Key: key, Upd: currUpd}
				written++
			}
		}
	case tpch.REGION:
		region := data.Tables.Regions[dp.Region]
		key, currUpd = region.GetPrimaryKey(), crdt.SetValue{NewValue: region.ToBytes()}
		embMapUpd[written] = crdt.EmbMapUpdate{Key: key, Upd: currUpd}
		written++
	}
	endTs := time.Now().UnixNano()
	fmt.Printf("[TPCH-DL]Took %d ms to prepare CRDT %s updates (%d_%d). Range: [%d:%d[.\n",
		(endTs-startTs)/int64(time.Millisecond), tpch.TableNames[tableI], tableI, offset, start, end)
	updChan <- crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(tpch.TableNames[tableI], proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapFirstUpdate{Upds: embMapUpd[:written]}}
}

func getLineitemsNRoutines() int {
	return 1 //Now that we use an array to represent lineitems, better do this with only 1 routine.
	//return tpch.TableEntries[tpch.LINEITEM]/250500 + 1
	//return tpch.TableEntries[tpch.LINEITEM] / 501000
	//return tpch.TableEntries[tpch.LINEITEM]/1020000 + 1
}

// Keeps track for which tables have the "processTables" and "preparePartitionedTables" already finished, in order to clean the raw table data
// Note: Not needed when creating updates using processed data.
func cleanRoutine() {
	//In theory I could use big.Int as a bitset, but I am afraid the SetBit's interface is not efficient.
	repliesPerTable := make([]uint8, len(data.RawTables)) //Stores the number of confirmations (i.e., processes that finished) for each file. +1 because of split lineitems
	var tableN int

	for nRepliesLeft := tpch.NTotalFiles * 2; nRepliesLeft > 0; nRepliesLeft-- {
		tableN = <-cleanChan
		repliesPerTable[tableN]++
		if repliesPerTable[tableN] == 2 { //If we got two confirmations, we can clean the raw table data
			fmt.Printf("[TPCH-DL]Cleaning raw data for table %s (%d)\n", tpch.GetTableName(tableN), tpch.GetSplitOffset(tableN))
			data.CleanTableRawData(tableN)
		}
	}
	data.FinishCleanTableRawData()
	//data.CleanProcTables() //TODO: Remove this.
	fmt.Printf("[TPCH-DL]All raw data has been clean.\n")
}

// Inner most updates: the object/entry itself (upd to an RWEmbMap, whose entries are LWWRegisters)
func GetInnerMapEntry(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd crdt.EmbMapUpdateAll) {
	entries := make(map[string]crdt.UpdateArguments, len(toRead))
	for _, tableI := range toRead {
		entries[headers[tableI]] = crdt.SetValue{NewValue: object[tableI]}
	}

	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(object[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], crdt.EmbMapUpdateAll{Upds: entries}
}

/*
	func GetInnerMapEntryArray(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd crdt.EmbMapUpdateAllArray) {
		entries := make([]crdt.EmbMapUpdate, len(toRead))
		for i, tableI := range toRead {
			entries[i] = crdt.EmbMapUpdate{Key: headers[tableI], Upd: crdt.SetValue{NewValue: object[tableI]}}
		}

		var buf strings.Builder
		for _, keyIndex := range primKeys {
			buf.WriteString(object[keyIndex])
			//TODO: Remove, just for easier debug
			buf.WriteRune('_')
		}
		//TODO: Also remove the slicing after removing the "_"
		return buf.String()[:buf.Len()-1], crdt.EmbMapUpdateAllArray{Upds: entries}
	}
*/
func GetInnerMapEntryArray(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd crdt.MultiArraySetRegister) {
	upd = make([][]byte, len(toRead))
	for i, tableI := range toRead {
		upd[i] = []byte(object[tableI])
	}
	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(object[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], upd
}

func GetInnerMapEntryCompactArray(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd crdt.CompactArraySetArray) {
	upd = make([]any, len(toRead))
	for i, tableI := range toRead {
		upd[i] = object[tableI]
	}
	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(object[keyIndex])
		buf.WriteRune('_') //Consider removing this (and then also the slicing below)
	}
	return buf.String()[:buf.Len()-1], upd
}

func GetInnerMapEntryStringArray(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd crdt.StringArraySetArrayInitialize) {
	/*if len(toRead) == len(object) {
		upd = object
	} else {
		upd = make([]string, len(toRead))
		for i, tableI := range toRead {
			upd[i] = object[tableI]
		}
	}*/
	upd = make([]string, len(toRead))
	for i, tableI := range toRead {
		//upd[i] = object[tableI]
		upd[i] = strings.Clone(object[tableI]) //Since original data comes from a big, fat array (Split still uses the same array), we clone so that the old array can be GC'ed
	}
	var buf strings.Builder
	for k, keyIndex := range primKeys {
		buf.WriteString(object[keyIndex])
		if k < len(primKeys)-1 {
			buf.WriteRune('_') //Note: This is needed, as otherwise we could mix different lineitems as one. E.g: partID = 101, suppID = 10; partID = 10, suppID = 110. Combined they are the same.
		}
	}
	return buf.String(), upd
}

// Returns the number of updates (UpdateObjectParams) issued for each table.
func GetNUpdatesOfTable(tableI int) int {
	if tpch.IsLineItemTable(tableI) {
		return tools.Min(getLineitemsNRoutines(), data.GetLineItemFilesCount())
	}
	return tpch.GetNFilesOfTable(tableI)
}

// Pre: oldNum and newNum have the same number of digits. Also, newNum > oldNum
func UpdateIncreasingNumberStringBuf(oldNum, newNum int32, buf []byte) {
	diff := newNum - oldNum
	//fmt.Printf("[TPCH-DL][NumUpd]OldNum: %d. NewNum: %d. Diff: %d. Buf len: %d.\n", oldNum, newNum, diff, len(buf))
	lastPos := len(buf) - 1
	if diff <= 9 { //Most common case
		buf[lastPos] += byte(diff)
		if buf[lastPos] > '9' { //Carry
			buf[lastPos] = buf[lastPos]%('9'+1) + '0' //E.g., if it was 3 steps after '9', it becomes '2'
			buf[lastPos-1]++
			for i := lastPos - 1; buf[i] > '9'; i-- { //i > 0 verification is not needed as oldNum and newNum have the same number of digits
				buf[i] = '0'
				buf[i-1]++
			}
		}
	} else if diff <= 99 {
		buf[lastPos] += byte(diff % 10)
		secLastPos := lastPos - 1
		if buf[lastPos] > '9' { //Carry
			buf[lastPos] = buf[lastPos]%('9'+1) + '0' //E.g., if it was 3 steps after '9', it becomes '2'
			buf[secLastPos] += byte(diff/10) + 1      //+1 because of the carry
		} else {
			buf[secLastPos] += byte(diff / 10)
		}
		if buf[secLastPos] > '9' { //Carry
			buf[secLastPos] = buf[secLastPos]%('9'+1) + '0'
			buf[secLastPos-1]++
			for i := secLastPos - 1; buf[i] > '9'; i-- {
				buf[i] = '0'
				buf[i-1]++
			}
		}
	} else { //Default to just appending. We don't need this case for now.
		strconv.AppendInt(buf[:0], int64(newNum), 10)
	}
}

/*func GetInnerMapEntryBytesArray(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd crdt.ByteArraySetDataInitialize) {
	upd = make([][]byte, len(toRead))
	for i, tableI := range toRead {
		upd[i] = []byte(object[tableI])
	}
	var buf strings.Builder
	for k, keyIndex := range primKeys {
		buf.WriteString(object[keyIndex])
		if k < len(primKeys)-1 {
			buf.WriteRune('_') //Note: This is needed, as otherwise we could mix different lineitems as one. E.g: partID = 101, suppID = 10; partID = 10, suppID = 110. Combined they are the same.
		}
	}
	return buf.String(), upd
}*/

//Memory-efficient opportunity: we can store data using not the string but instead the respective data type.
//Let's see this for LineItem, comparing datatypes to string:
//ORDERKEY, PARTKEY and SUPPKEY are more efficient if > 9999. Which will happen.
//Date is 4 bytes, which is always better
//Quantity and linenumber are 1 byte, which is always better
//L_TAX and L_DISCOUNT are worse (8 vs 4 bytes.) L_EXTENDED price could in theory be stored as int32... still, it seems to use 7 digits so it's similar to float64.
//RETURNFLAG and LINESTATUS can be 1 byte, so better.
//SHIPINSTRUCT and SHIPMODE are 1 byte, so better.
//L_COMMENT is the same.
//Aside from comments, we can probably save the memory usage in like half?

//Also: LineItems do not actually need the start of each data. They could be stored as a simple RegisterCRDT. As only the comment is a String.
//Tempting...
//Orders and PartSupp also.
//Most others not but maybe I could just use a special character to mark the end of a string? Then I would use only 1 extra byte, and only for strings. Or even if two extra bytes, "still OK".
//Yep, sounds like the path to go.
//0x00 to 0x1F are control characters, so I can use them safely. 0x80 to 0xBF are continuation bytes, can be used to. Or 0xF5 to 0xFF are invalid, so they are even safer for this purpose.
