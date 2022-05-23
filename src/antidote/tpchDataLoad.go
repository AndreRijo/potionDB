package antidote

import (
	fmt "fmt"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strings"
	"time"
	"tpch_data/tpch"
)

type DataloadParameters struct {
	Region    int8
	IsTMReady chan bool
	Tm        *TransactionManager
	Sf        float64
	DataLoc   string
}

const (
	PART_BKT_INDEX = 5
)

var (
	data        tpch.TpchData
	dp          DataloadParameters
	buckets     = []string{"R1", "R2", "R3", "R4", "R5", "PART"}
	regionFuncs [8]func([]string) int8
)

//TODO: CRDT_PER_OBJ option

func LoadData(dataP DataloadParameters) {
	data, dp = tpch.TpchData{TpchConfigs: tpch.TpchConfigs{Sf: dataP.Sf, DataLoc: dataP.DataLoc}}, dataP
	//TODO: Clean unecessary data
	fmt.Println("[TPCH-DL]Starting to prepare base data...")
	start := time.Now().UnixNano()
	data.PrepareBaseData()
	end := time.Now().UnixNano()
	fmt.Printf("[TPCH-DL]Base data read and prepared. Time taken: %dms\n", (end-start)/int64(time.Millisecond))
	//Part and lineitem are nil
	regionFuncs = [8]func([]string) int8{data.ProcTables.CustSliceToRegion, nil, data.ProcTables.NationSliceToRegion, data.ProcTables.OrdersSliceToRegion, nil,
		data.ProcTables.PartSuppSliceToRegion, data.ProcTables.RegionSliceToRegion, data.ProcTables.SupplierSliceToRegion}
	fmt.Println("[TPCH-DL]Starting to prepare updates...")
	upds := PrepareCrdtUpdates()
	fmt.Println("[TPCH-DL]Starting to send base data...")
	SendBaseData(upds)
	data.CleanAll()
}

func SendBaseData(upds []*UpdateObjectParams) {
	<-dp.IsTMReady //Wait until TM is ready
	start := time.Now().UnixNano()
	fmt.Println("[TPCH-DL]TM ready, starting to send data")
	ic := InternalClient{}.Initialize(dp.Tm)
	ic.DoUpdate(upds)
	end := time.Now().UnixNano()
	fmt.Printf("[TPCH-DL]Done, TM commited. Time taken: %dms\n", (end-start)/int64(time.Millisecond))
	//debugRead(ic)
}

func debugRead(tc InternalClient) {
	/*
		tableRead := ReadObjectParams{
			KeyParams: KeyParams{Key: tpch.TableNames[tpch.CUSTOMER], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[dp.Region]},
			ReadArgs:  crdt.EmbMapPartialArguments{Args: crdt.GetValueArguments{Key: }},
		}
	*/
	fmt.Println("Sleeping for 15s before reading...")
	time.Sleep(15 * time.Second)
	tableRead := ReadObjectParams{
		KeyParams: KeyParams{Key: tpch.TableNames[tpch.SUPPLIER], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[dp.Region]},
		ReadArgs:  crdt.StateReadArguments{},
	}
	state := tc.DoRead([]ReadObjectParams{tableRead})
	fmt.Printf("Read result: %T %v\n", state[0], state[0])
}

func PrepareCrdtUpdates() []*UpdateObjectParams {
	//Need 3 types:
	//Replicated everywhere (parts) (TODO: This can only be loaded in one server and replicated to others so that all have the same version)
	//Replicated locally (order, supplier, itemsupply, probably some other I'm forgetting)
	//Replicated locally + one other place (lineitem)

	//Call for each table
	//Then call for each object in the table

	//Afterwards, need to prepare indexes
	//Or maybe... have the dataload take care of the indexes? They take like 1-2s to send to the servers.
	//Advantage: much less work
	//Disadvantage: has to wait for servers to be ready.

	//For now, let's keep it simple. Take care of only tables
	startTime := time.Now().UnixNano()
	regionBkt := buckets[dp.Region]
	upds := make([]*UpdateObjectParams, 7) //Lineitems will be added with append
	//Need to do these first
	upds[0] = preparePartionedTables(tpch.REGION, regionBkt)
	upds[1] = preparePartionedTables(tpch.NATION, regionBkt)
	upds[2] = preparePartionedTables(tpch.SUPPLIER, regionBkt)
	upds[3] = preparePartionedTables(tpch.CUSTOMER, regionBkt)
	upds[4] = preparePartionedTables(tpch.ORDERS, regionBkt)
	//Lineitem is very time consuming so we do it in a different way, using multiple goroutines
	lineStartTime := time.Now().UnixNano()
	lineChan := make(chan []*UpdateObjectParams, 1)
	go prepareMultiPartionedTables(tpch.LINEITEM, regionBkt, lineChan)
	upds[5] = preparePartionedTables(tpch.PARTSUPP, regionBkt)
	upds[6] = prepareTableEverywhere(tpch.PART, buckets[PART_BKT_INDEX])
	endBaseTime := time.Now().UnixNano()
	//upds[7] = prepareMultiPartionedTables(tpch.LINEITEM, regionBkt)
	upds = append(upds, <-lineChan...)
	endTime := time.Now().UnixNano()
	loadBeforeLine, lineLoad, totalLoad := endBaseTime-startTime, endTime-lineStartTime, endTime-startTime
	//loadBeforeLine, lineLoad, totalLoad := lineStartTime-startTime, endTime-lineStartTime, endTime-startTime
	fmt.Printf("[TPCH-DL]Upds preparation times: basic tables %dms, lineitems %dms, all %dms\n", loadBeforeLine/int64(time.Millisecond),
		lineLoad/int64(time.Millisecond), totalLoad/int64(time.Millisecond))
	return upds
}

func prepareMultiPartionedTables(tableI int, bucket string, resultChan chan []*UpdateObjectParams) {
	//TODO: Optimize this with goroutines like it is done in clientDataLoad.go
	//Not sure if it is needed, this takes about 4-5s with SF = 1.
	table, toRead, keys, header, name, regFunc := data.Tables[tableI], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI], data.ProcTables.LineitemSliceToRegion

	nRoutines := len(table)/500000 + 1
	endChan, objParams := make(chan *UpdateObjectParams, nRoutines), make([]*UpdateObjectParams, nRoutines)
	if nRoutines == 1 {
		//No extra goroutine
		multiPartionedTableHelper(table, toRead, keys, header, name, bucket, regFunc, endChan)
		objParams[0] = <-endChan
		resultChan <- objParams
	}
	factor := len(table) / nRoutines
	startI, endI := 0, factor
	for i := 0; i < nRoutines; i++ {
		slicedTable := table[startI:endI]
		go multiPartionedTableHelper(slicedTable, toRead, keys, header, name, bucket, regFunc, endChan)
		startI = endI
		endI += factor
		if i == nRoutines-2 {
			endI = len(table)
		}
	}
	for i := 0; i < nRoutines; i++ {
		objParams[i] = <-endChan
	}

	resultChan <- objParams
}

func multiPartionedTableHelper(table [][]string, toRead []int8, keys []int, header []string, name string, bucket string,
	regFunc func(obj []string) []int8, endChan chan *UpdateObjectParams) {
	embMapUpd := make(map[string]crdt.UpdateArguments)
	for _, obj := range table {
		regs := regFunc(obj)
		if regs[0] == dp.Region || len(regs) == 2 && regs[1] == dp.Region {
			//If one of the regions is of this server, create the item
			key, innerUpd := getInnerMapEntry(header, keys, obj, toRead) //Update for one entry in the map
			embMapUpd[key] = *innerUpd
		}
	}
	var finalUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: embMapUpd}
	endChan <- &UpdateObjectParams{KeyParams: CreateKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: &finalUpd}
	//return &UpdateObjectParams{KeyParams: CreateKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: &finalUpd}
}

func preparePartionedTables(tableI int, bucket string) *UpdateObjectParams {
	table, toRead, keys, header, name, regFunc := data.Tables[tableI], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI], regionFuncs[tableI]
	embMapUpd := make(map[string]crdt.UpdateArguments) //Update for the embedded map
	for _, obj := range table {
		if regFunc(obj) == dp.Region {
			key, innerUpd := getInnerMapEntry(header, keys, obj, toRead) //Update for one entry in the map
			embMapUpd[key] = *innerUpd
		}
		//Ignore if it's not of the region of this server
	}
	var finalUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: embMapUpd}
	return &UpdateObjectParams{KeyParams: CreateKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: &finalUpd}

}

//For tables that are to be replicated in every server and thus need no filtering
func prepareTableEverywhere(tableI int, bucket string) *UpdateObjectParams {
	table, toRead, keys, header, name := data.Tables[tableI], data.ToRead[tableI], data.Keys[tableI], data.Headers[tableI], tpch.TableNames[tableI]
	embMapUpd := make(map[string]crdt.UpdateArguments) //Update for the embedded map
	for _, obj := range table {
		key, innerUpd := getInnerMapEntry(header, keys, obj, toRead) //Update for one entry in the map
		embMapUpd[key] = *innerUpd
	}
	var finalUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: embMapUpd}
	return &UpdateObjectParams{KeyParams: CreateKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: &finalUpd}
}

//Same as getEntryUpd of clientDataLoad (tpch_client)
func getInnerMapEntry(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd *crdt.EmbMapUpdateAll) {
	entries := make(map[string]crdt.UpdateArguments)
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
	return buf.String()[:buf.Len()-1], &crdt.EmbMapUpdateAll{Upds: entries}
}
