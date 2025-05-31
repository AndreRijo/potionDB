package components

import (
	fmt "fmt"
	"strings"
	"time"

	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	tpch "tpch_data_processor/tpch"
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
	PART_BKT_INDEX = 5
)

var (
	data        *tpch.TpchData
	dp          DataloadParameters
	regionFuncs [8]func([]string) int8
	updChan     chan crdt.UpdateObjectParams
	iCfg        tpch.IndexConfigs
	ic          InternalClient
	startTime   int64
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
	fmt.Println("[TPCH-DL]Start time of dataload: ", start.Format("2006-01-02 15:04:05.000"))
	LoadBaseData(dataP)
}

func LoadBaseData(dataP DataloadParameters) {
	data, dp = &tpch.TpchData{TpchConfigs: tpch.TpchConfigs{Sf: dataP.Sf, DataLoc: dataP.DataLoc, IsSingleServer: false}, Tables: &tpch.Tables{NOrders: int(float64(tpch.TableEntries[tpch.ORDERS]) * dp.Sf)}}, dataP
	//Part and lineitem are nil
	regionFuncs = [8]func([]string) int8{data.Tables.CustSliceToRegion, nil, data.Tables.NationSliceToRegion, data.Tables.OrdersSliceToRegion, nil,
		data.Tables.PartSuppSliceToRegion, data.Tables.RegionSliceToRegion, data.Tables.SupplierSliceToRegion}
	data.Initialize()
	go PrepareBaseDataUpdsAndSend()
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
	tpch.InitializeIndexInfo(iCfg, data.Tables)
	tpch.PrepareIndexes()
	fmt.Printf("[TPCH-DL]Finished loading and preparing index. My Region: %d. Current time: %v.\n", dp.Region, time.Now().Format("2006-01-02 15:04:05.000"))
}

func SendIndexData() {
	confirmChan := make(chan bool, len(iCfg.QueryNumbers))
	for i := 1; i <= len(iCfg.QueryNumbers); i++ {
		multiUpdHelper(<-iCfg.GlobalUpdsChan, confirmChan)
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
	for i := 0; i < len(iCfg.QueryNumbers); i++ {
		<-confirmChan
	}
	fmt.Println("[TPCH-DL]Time at which all index updates were sent: ", time.Now().Format("2006-01-02 15:04:05.000"))
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
	fmt.Println("[TPCH-DL]Starting to prepare updates...")
	upds := PrepareCrdtUpdates()
	fmt.Println("[TPCH-DL]Starting to send base data...")
	SendBaseData(upds)
	//fmt.Println("[TPCH-DL]IsGlobal:", dp.IndexConfigs.IsGlobal)
	if dp.IndexConfigs.IsGlobal {
		go SendIndexData()
	} else {
		go SendLocalIndexData()
	}
	data.CleanAll()
}

func PrepareCrdtUpdates() []crdt.UpdateObjectParams {
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
		}
		fmt.Println("[TPCH-DL]Preparing protos", tpch.TableNames[tableN], tableN)
		if tableN == tpch.PART {
			go prepareTableEverywhere(tableN, tpch.Buckets[PART_BKT_INDEX])
		} else if tableN == tpch.LINEITEM {
			go prepareMultiPartionedTables(tpch.LINEITEM, regionBkt)
		} else {
			go preparePartionedTables(tableN, regionBkt)
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
	fmt.Printf("[TPCH-DL]Finished creating all data protos at %s. Took %dms. Time taken since DL start: %dms\n", end.Format("15:04:05.000"), (end.UnixNano()/1000000)-start, (end.UnixNano()/1000000)-startTime)
	return upds
}

func SendBaseData(upds []crdt.UpdateObjectParams) {
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

func prepareMultiPartionedTables(tableI int, bucket string) {
	table, toRead, keys, header, name, regFunc := data.RawTables[tableI], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI], data.Tables.LineitemSliceToRegion

	//nRoutines := len(table)/7000000 + 1
	//nRoutines := len(table)/1510000 + 1
	nRoutines := getLineitemsNRoutines()
	//fmt.Printf("[TPCH-DL]Using %d goroutines to prepare lineitem data\n", nRoutines)
	//nRoutines := 1
	if nRoutines == 1 {
		//No extra goroutine
		multiPartionedTableHelper(table, toRead, keys, header, name, bucket, regFunc)
		return
	}
	factor := len(table) / nRoutines
	startI, endI := 0, factor
	for i := 0; i < nRoutines; i++ {
		slicedTable := table[startI:endI]
		go multiPartionedTableHelper(slicedTable, toRead, keys, header, name, bucket, regFunc)
		startI = endI
		endI += factor
		if i == nRoutines-2 {
			endI = len(table)
		}
	}
}

// For lineitems with two regions, each server only instanciates the version corresponding to its region.
func multiPartionedTableHelper(table [][]string, toRead []int8, keys []int, header []string, name string, bucket string,
	regFunc func(obj []string) []int8) { //, endChan chan *UpdateObjectParams) {

	start := time.Now().UnixNano()
	var embMapUpd []crdt.EmbMapUpdate
	if dp.Sf >= 0.1 {
		embMapUpd = make([]crdt.EmbMapUpdate, int(float64(len(table))*0.4))
	} else {
		embMapUpd = make([]crdt.EmbMapUpdate, int(float64(len(table))))
	}
	i := 0
	//embMapUpd, i := make([]crdt.EmbMapUpdate, len(table)*2), 0
	for _, obj := range table {
		regs := regFunc(obj)
		if regs[0] == dp.Region || len(regs) == 2 && regs[1] == dp.Region {
			//If one of the regions is of this server, create the item
			key, innerUpd := GetInnerMapEntryArray(header, keys, obj, toRead) //Update for one entry in the map
			embMapUpd[i] = crdt.EmbMapUpdate{Key: key, Upd: innerUpd}
			i++
		}
	}
	end := time.Now().UnixNano()
	fmt.Printf("[TPCH-DL]Time taken to prepare lineitem data: %dms\n", (end-start)/int64(time.Millisecond))
	updChan <- crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapUpdateAllArray{Upds: embMapUpd[:i]}}
}

// func preparePartionedTables(tableI int, bucket string) *UpdateObjectParams {
func preparePartionedTables(tableI int, bucket string) {
	table, toRead, keys, header, name, regFunc := data.RawTables[tableI], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI], regionFuncs[tableI]
	var embMapUpd []crdt.EmbMapUpdate
	if dp.Sf >= 0.1 {
		embMapUpd = make([]crdt.EmbMapUpdate, len(table)/4)
	} else {
		embMapUpd = make([]crdt.EmbMapUpdate, len(table)/3)
	}
	i := 0
	//embMapUpd, i := make([]crdt.EmbMapUpdate, len(table)), 0
	for _, obj := range table {
		if regFunc(obj) == dp.Region {
			key, innerUpd := GetInnerMapEntryArray(header, keys, obj, toRead) //Update for one entry in the map
			embMapUpd[i] = crdt.EmbMapUpdate{Key: key, Upd: innerUpd}
			i++
		}
		//Ignore if it's not of the region of this server
		//ignore(regFunc)
	}
	updChan <- crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapUpdateAllArray{Upds: embMapUpd[:i]}}
}

// For tables that are to be replicated in every server and thus need no filtering
// For this type of tables (PART), each server loads a portion of the data, based on its region and number of regions.
// func prepareTableEverywhere(tableI int, bucket string) *UpdateObjectParams {
func prepareTableEverywhere(tableI int, bucket string) {
	table, toRead, keys, header, name := data.RawTables[tableI], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI]
	nRegions, myRegion, tableLen := len(data.Tables.Regions), int(dp.Region), len(table)
	startI, finishI := (tableLen/nRegions)*myRegion, (tableLen/nRegions)*(myRegion+1)
	//startI, finishI := 0, tableLen
	embMapUpd, i := make([]crdt.EmbMapUpdate, len(table)/5), 0
	//embMapUpd, i := make([]crdt.EmbMapUpdate, len(table)), 0
	table = table[startI:finishI]
	for _, obj := range table {
		key, innerUpd := GetInnerMapEntryArray(header, keys, obj, toRead) //Update for one entry in the map
		embMapUpd[i] = crdt.EmbMapUpdate{Key: key, Upd: innerUpd}
		i++
	}
	updChan <- crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapUpdateAllArray{Upds: embMapUpd[:i]}}
	ignore(nRegions, myRegion)
}

func getLineitemsNRoutines() int {
	return 1 //Now that we use an array to represent lineitems, better do this with only 1 routine.
	//return tpch.TableEntries[tpch.LINEITEM]/250500 + 1
	//return tpch.TableEntries[tpch.LINEITEM] / 501000
	//return tpch.TableEntries[tpch.LINEITEM]/1020000 + 1
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
